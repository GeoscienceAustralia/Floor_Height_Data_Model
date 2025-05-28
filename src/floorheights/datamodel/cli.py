import json
import uuid
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
import rasterio
from shapely.geometry import box
from sqlalchemy import JSON, UUID, LargeBinary, Numeric, String, Table, select
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

from floorheights.datamodel import etl
from floorheights.datamodel.models import (
    AddressPoint,
    Building,
    SessionLocal,
)


@click.group()
def cli():
    pass


@click.command()
def create_dummy_address_point():
    """Create dummy address point"""
    session = SessionLocal()
    address = AddressPoint(
        gnaf_id="GANSW717206574",
        address="2 BENTLEY PLACE, WAGGA WAGGA, NSW 2650",
        location="SRID=4326;POINT(147.377214 -35.114780)",
    )
    session.add(address)
    session.commit()

    click.echo("Dummy address added")


@click.command()
def create_dummy_building():
    """Create a dummy building"""
    session = SessionLocal()
    building = Building(
        outline=(
            "SRID=4326;"
            "POLYGON ((147.37761655448156 -35.11448724509989, 147.37778526244756 -35.11466902926723,"
            "147.37788066971024 -35.11463666981137, 147.37775733837083 -35.11443775405107, "
            "147.37761655448156 -35.11448724509989))"
        ),
        min_height_ahd=179.907,
        max_height_ahd=180.155,
    )
    session.add(building)
    session.commit()

    click.echo("Dummy building added")


@click.command()
@click.option("-i", "--input-address", required=True, type=str, help="Input address points file path.")  # fmt: skip
@click.option("-c", "--chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
def ingest_address_points(input_address: str, chunksize: int):
    """Ingest address points"""
    click.secho("Ingesting address points", bold=True)
    try:
        click.echo("Loading address points...")
        address = etl.read_ogr_file(
            input_address,
            columns=["ADDRESS_DETAIL_PID", "COMPLETE_ADDRESS", "GEOCODE_TYPE"],
        )
    except Exception as error:
        raise click.exceptions.FileError(Path(input_address).name, error)

    address = address[
        (address.GEOCODE_TYPE == "BUILDING CENTROID")
        | (address.GEOCODE_TYPE == "PROPERTY CENTROID")
    ]
    address = address.rename(
        columns={
            "COMPLETE_ADDRESS": "address",
            "ADDRESS_DETAIL_PID": "gnaf_id",
            "GEOCODE_TYPE": "geocode_type",
        }
    )
    address = address.rename_geometry("location")

    click.echo("Copying to PostgreSQL...")
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        address.to_postgis(
            "address_point",
            conn,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=chunksize,
        )
        click.echo("Address ingestion complete")


@click.command()
@click.option("-i", "--input-buildings", required=True, type=str, help="Input building footprint file path.")  # fmt: skip
@click.option("-d", "--input-dem", required=True, type=click.File(), help="Input DEM file path, used to sample building footprint ground height.")  # fmt: skip
@click.option("-s", "--chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
@click.option("--split-by-cadastre", type=str, help="Split buildings by cadastre, specify input cadastre vector file path for splitting.")  # fmt: skip
@click.option("--join-land-zoning", type=str, help="Join land zoning type to buildings, specify input land zoning vector file path.")  # fmt: skip
@click.option("--land-zoning-field", type=str, help="The land zoning dataset's field name to join to buildings.")  # fmt: skip
@click.option("--remove-small", type=float, is_flag=False, flag_value=30, default=None, help="Remove smaller buildings, optionally specify an area threshold in square metres.  [default: 30.0]")  # fmt: skip
@click.option("--remove-overlapping", type=float, is_flag=True, flag_value=0.80, default=None, help="Remove overlapping buildings, optionally specify an intersection ratio threshold.  [default: 0.80]")  # fmt: skip
def ingest_buildings(
    input_buildings: str,
    input_dem: click.File,
    chunksize: int,
    split_by_cadastre: str,
    join_land_zoning: str,
    land_zoning_field: str,
    remove_small: float,
    remove_overlapping: float,
):
    """Ingest building footprints"""
    if join_land_zoning and not land_zoning_field:
        raise click.UsageError(
            "--join-land-zoning must be used with --land-zoning-field"
        )

    click.secho("Ingesting building footprints", bold=True)
    try:
        click.echo("Loading DEM...")
        dem = rasterio.open(input_dem.name)
        dem_crs = dem.crs
    except Exception as error:
        raise click.exceptions.FileError(input_dem.name, error)

    click.echo("Creating mask...")
    bounds = dem.bounds
    mask_geom = box(*bounds)
    mask_df = gpd.GeoDataFrame(
        {"id": 1, "geometry": [mask_geom]}, crs=dem_crs.to_string()
    )
    # Transform mask to WGS84 - might be slightly offset buildings are in GDA94/GDA2020
    mask_df = mask_df.to_crs(4326)
    mask_bbox = mask_df.total_bounds

    click.echo("Loading building footprints...")
    try:
        buildings = etl.read_ogr_file(
            input_buildings, columns=["geometry"], bbox=tuple(mask_bbox)
        )
    except Exception as error:
        raise click.exceptions.FileError(input_buildings, error)

    buildings = buildings[buildings.geom_type == "Polygon"]  # Remove multipolygons
    buildings = buildings.to_crs(
        dem_crs.to_epsg()
    )  # Transform buildings to CRS of our DEM

    if split_by_cadastre:
        click.echo("Splitting buildings by cadastre...")
        try:
            cadastre = etl.read_ogr_file(split_by_cadastre, columns=["geometry"])
            cadastre = cadastre.to_crs(dem.crs)
        except Exception as error:
            raise click.exceptions.FileError(Path(split_by_cadastre).name, error)

        session = SessionLocal()
        with session.begin():
            conn = session.connection()
            try:
                # Get address points from db
                address_points = gpd.read_postgis(
                    select(AddressPoint),
                    conn,
                    geom_col="location",
                )
                # Check if the addresses are empty for the area of interest
                address_points = address_points.to_crs(dem.crs)
                if not address_points.within(mask_df.geometry.iloc[0]).any:
                    raise Exception
            except Exception:
                raise click.exceptions.BadParameter(
                    "--split-by-cadastre can only be used after ingesting address_points."
                )

            buildings = etl.split_by_cadastre(address_points, buildings, cadastre)

    if remove_small:
        click.echo(f"Removing buildings < {remove_small} m^2...")
        bool_mask = buildings.area > remove_small
        buildings = buildings[bool_mask]
        remove_count = (bool_mask).value_counts()[False]
        click.echo(f"Removed {remove_count} buildings...")

    if join_land_zoning:
        click.echo("Joining land zoning attribute...")
        try:
            land_use = etl.read_ogr_file(join_land_zoning, mask=mask_df)
            land_use = land_use.to_crs(dem.crs)

            if land_zoning_field not in land_use.columns:
                raise click.exceptions.BadParameter(
                    f"Field '{land_zoning_field}' not found in land land zoning dataset"
                )

            # Sample land use zoning polygons with building footprints
            buildings = etl.sample_polys_with_buildings(
                land_use, buildings, land_zoning_field
            )
            buildings = buildings.rename(columns={land_zoning_field: "land_use_zone"})

        except Exception as error:
            raise click.exceptions.FileError(Path(join_land_zoning).name, error)

    click.echo("Sampling DEM with buildings...")
    min_heights, max_heights = etl.sample_dem_with_buildings(dem, buildings)
    buildings["min_height_ahd"] = min_heights
    buildings["max_height_ahd"] = max_heights

    # Drop rows outside the extent of the DEM
    buildings = buildings[buildings["min_height_ahd"].notna()]
    buildings = buildings[buildings["max_height_ahd"].notna()]

    buildings = buildings.round({"min_height_ahd": 3, "max_height_ahd": 3})

    # Remove any buildings that sample no data
    buildings = buildings[buildings["min_height_ahd"] != dem.nodata]
    buildings = buildings[buildings["max_height_ahd"] != dem.nodata]
    buildings = buildings.to_crs(4326)  # Transform back to WGS84
    buildings = buildings.rename_geometry("outline")

    click.echo("Copying to PostgreSQL...")
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        buildings.to_postgis(
            "building",
            conn,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=chunksize,
        )

        if remove_overlapping:
            click.echo("Removing overlapping buildings...")
            result = etl.remove_overlapping_geoms(
                session, remove_overlapping, bbox=mask_bbox.tolist()
            )
            click.echo(f"Removed {result.rowcount} overlapping buildings...")

        click.echo("Building ingestion complete")


@click.command()
@click.option("-c", "--input-cadastre", type=str, help="Input cadastre vector file path to support address joining.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join addresses to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
def join_address_buildings(
    input_cadastre: str, flatten_cadastre: bool, join_largest: bool
):
    """Join address points to building outlines"""
    if join_largest and not input_cadastre:
        raise click.UsageError(
            "--join-largest-building must be used with --input-cadastre"
        )
    if flatten_cadastre and not input_cadastre:
        raise click.UsageError("--flatten-cadastre must be used with --input-cadastre")

    click.secho("Joining addresses to buildings", bold=True)
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()

        click.echo("Performing join by knn...")
        # Selects address-building matches for addresses geocoded to building centroids
        select_query = etl.build_address_match_query(
            join_by="knn", knn_max_distance=5
        ).where(AddressPoint.geocode_type == "BUILDING CENTROID")
        etl.insert_address_building_association(session, select_query)

        if input_cadastre:
            click.echo("Loading cadastre...")
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(Path(input_cadastre).name, error)

            click.echo("Copying cadastre to PostgreSQL...")
            cadastre_gdf.to_postgis(
                "temp_cadastre",
                conn,
                schema="public",
                if_exists="replace",
                index=True,
                index_label="id",
            )
            # Get temp_cadastre table model from database
            temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=conn)

            if flatten_cadastre:
                click.echo("Flattening cadastre geometries...")
                temp_cadastre = etl.flatten_cadastre_geoms(
                    session, conn, Base, temp_cadastre
                )

            click.echo("Performing join with cadastre...")
            # Selects address-building matches by joining to common cadastre lots for
            # addresses geocoded to property centroids
            select_query = etl.build_address_match_query("cadastre", temp_cadastre)

            if join_largest:
                click.echo("Joining with largest building on lot...")
                # Modify select to join to largest building on the lot for distinct address
                select_query = select_query.order_by(
                    AddressPoint.id, func.ST_Area(Building.outline).desc()
                ).distinct(AddressPoint.id)
            etl.insert_address_building_association(session, select_query)

            # Finish up by joining addresses to nearest-neighbour buildings
            # that aren't within the cadastre and are within a distance threshold
            select_query = etl.build_address_match_query("knn", temp_cadastre, 10)
            etl.insert_address_building_association(session, select_query)
        else:
            # If we don't use a cadastre, do a nearest-neighbour join
            select_query = etl.build_address_match_query("knn")
            etl.insert_address_building_association(session, select_query, 10)

        if input_cadastre:
            temp_cadastre.drop(conn)

    click.echo("Joining complete")


@click.command()
@click.option("-i", "--input-nexis", required=True, type=str, help="Input NEXIS CSV file path.")  # fmt: skip
@click.option("-c", "--input-cadastre", required=False, type=str, default=None, help="Input cadastre OGR dataset file path to support joining non-GNAF NEXIS points.")  # fmt: skip
@click.option("--clip-to-cadastre", is_flag=True, help="Clip measure points by the cadastre dataset extents. This will speed up processing, but won't join measures outside the extent of the cadastre.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measure points to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
def ingest_nexis_measures(
    input_nexis: str,
    clip_to_cadastre: str,
    flatten_cadastre: bool,
    join_largest: bool,
    input_cadastre: bool,
):
    """Ingest NEXIS floor height measures"""
    if clip_to_cadastre and not input_cadastre:
        raise click.UsageError("--clip-to-cadastre must be used with --input-cadastre")
    if join_largest and not input_cadastre:
        raise click.UsageError(
            "--join-largest-building must be used with --input-cadastre"
        )
    if flatten_cadastre and not input_cadastre:
        raise click.UsageError("--flatten-cadastre must be used with --input-cadastre")

    click.secho("Ingesting NEXIS measures", bold=True)
    try:
        click.echo("Loading NEXIS points...")
        nexis_gdf = etl.read_nexis_csv(input_nexis, 4283)
    except Exception as error:
        raise click.exceptions.FileError(input_nexis, error)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()

        if input_cadastre:
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(Path(input_cadastre).name, error)
            if clip_to_cadastre:
                click.echo("Clipping NEXIS points to cadastre...")
                # Clip NEXIS points to cadastre extent
                nexis_gdf = gpd.clip(nexis_gdf, cadastre_gdf)
        else:
            # Subset NEXIS points based GNAF IDs in the database
            gnaf_ids = session.execute(select(AddressPoint.gnaf_id)).all()
            gnaf_ids = [row[0] for row in gnaf_ids]
            nexis_gdf = nexis_gdf[nexis_gdf["lid"].isin(gnaf_ids)]

        nexis_gdf["id"] = [uuid.uuid4() for _ in range(len(nexis_gdf.index))]
        nexis_gdf = nexis_gdf.set_index(["id"])

        click.echo("Copying NEXIS points to PostgreSQL...")
        nexis_gdf.to_postgis(
            "temp_nexis",
            conn,
            schema="public",
            if_exists="replace",
            index=True,
            dtype={"id": UUID, "floor_height_m": Numeric},
        )
        temp_nexis = Table("temp_nexis", Base.metadata, autoload_with=conn)

        # Build select query to insert into the floor_measure table for GNAF ID matches
        method_id = etl.get_or_create_method_id(session, "Inverse transform sampling")
        modelled_query_gnaf = etl.build_floor_measure_query(
            temp_nexis,
            "floor_height_m",
            method_id,
            accuracy_measure=50,
            storey=0,
            join_by="gnaf_id",
            gnaf_id_col="lid",
        )

        click.echo("Inserting GNAF records into floor_measure table...")
        modelled_inserted_ids = etl.insert_floor_measure(session, modelled_query_gnaf)

        click.echo("Inserting non-GNAF records into floor_measure table...")
        # Build select queries to insert into the floor_measure table for non GNAF addresses
        # First, by point-building intersection
        modelled_query_intersect = etl.build_floor_measure_query(
            temp_nexis,
            "floor_height_m",
            method_id,
            accuracy_measure=50,
            storey=0,
            join_by="intersects",
        ).where(
            # Modify the query to select non-GNAF addresses
            temp_nexis.c.lid.notlike("GA%")
        )
        modelled_query_intersect_ids = etl.insert_floor_measure(
            session, modelled_query_intersect
        )

        modelled_inserted_ids += modelled_query_intersect_ids

        if input_cadastre:
            click.echo("Copying cadastre to PostgreSQL...")
            cadastre_gdf.to_postgis(
                "temp_cadastre",
                conn,
                schema="public",
                if_exists="replace",
                index=True,
                index_label="id",
            )
            temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=conn)

            if flatten_cadastre:
                click.echo("Flattening cadastre geometries...")
                temp_cadastre = etl.flatten_cadastre_geoms(
                    session, conn, Base, temp_cadastre
                )

            # Second, by joining to buildings with a common cadastre lot
            modelled_query_cadastre = etl.build_floor_measure_query(
                temp_nexis,
                "floor_height_m",
                method_id,
                accuracy_measure=50,
                storey=0,
                join_by="cadastre",
                cadastre=temp_cadastre,
            ).where(temp_nexis.c.lid.notlike("GA%"))

            if join_largest:
                click.echo("Joining with largest building on lot...")
                # Modify select to join to largest building on the lot for distinct points
                modelled_query_cadastre = modelled_query_cadastre.order_by(
                    temp_nexis.c.id, func.ST_Area(Building.outline).desc()
                ).distinct(temp_nexis.c.id)
            modelled_inserted_cadastre_ids = etl.insert_floor_measure(
                session, modelled_query_cadastre
            )

            # Third, by nearest-neighbour outside the extents of the cadastre
            modelled_query_cadastre_knn = etl.build_floor_measure_query(
                temp_nexis,
                "floor_height_m",
                method_id,
                accuracy_measure=50,
                storey=0,
                join_by="knn",
                cadastre=temp_cadastre,
            ).where(temp_nexis.c.lid.notlike("GA%"))
            modelled_inserted_cadastre_knn_ids = etl.insert_floor_measure(
                session, modelled_query_cadastre_knn
            )

            # Concat the inserted id lists
            modelled_inserted_ids += (
                modelled_inserted_cadastre_ids + modelled_inserted_cadastre_knn_ids
            )

            # List of tuples to list of ids
            modelled_inserted_ids = [id for (id,) in modelled_inserted_ids]

        if modelled_inserted_ids:
            nexis_dataset_id = etl.get_or_create_dataset_id(
                session, "NEXIS", "NEXIS flood exposure points", "Geoscience Australia"
            )
            etl.insert_floor_measure_dataset_association(
                session, nexis_dataset_id, modelled_inserted_ids
            )

        temp_nexis.drop(conn)
        if input_cadastre:
            temp_cadastre.drop(conn)

        click.echo("NEXIS ingestion complete")


@click.command()
@click.option("-i", "--input-data", required=True, type=str, help="Input validation points dataset file path.")  # fmt: skip
@click.option("-c", "--input-cadastre", type=str, help="Input cadastre OGR dataset file path to support address joining.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measures to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
@click.option("--ffh-field", type=str, required=True, help="Name of the first floor height field.")  # fmt: skip
@click.option("--step-size", type=float, default=0.28, show_default=True, help="Step size value in metres.")  # fmt: skip
@click.option("--dataset-name", type=str, default="Validation", show_default=True, help="The floor measure dataset name.")  # fmt: skip
@click.option("--dataset-desc", type=str, help="The floor measure dataset description.")  # fmt: skip
@click.option("--dataset-src", type=str, help="The floor measure dataset source.")  # fmt: skip
def ingest_validation_measures(
    input_data: str,
    input_cadastre: str,
    flatten_cadastre: bool,
    join_largest: bool,
    ffh_field: str,
    step_size: float,
    dataset_name: str,
    dataset_desc: str,
    dataset_src: str,
):
    """Ingest validation floor height method"""
    if join_largest and not input_cadastre:
        raise click.UsageError(
            "--join-largest-building must be used with --input-cadastre"
        )
    if flatten_cadastre and not input_cadastre:
        raise click.UsageError("--flatten-cadastre must be used with --input-cadastre")

    click.secho("Ingesting Validation measures", bold=True)
    try:
        click.echo("Loading validation points...")
        method_gdf = etl.read_ogr_file(input_data)
    except Exception as error:
        raise click.exceptions.FileError(Path(input_data).name, error)

    if ffh_field not in method_gdf.columns:
        raise click.exceptions.BadParameter(
            f"Field '{ffh_field}' not found in input file"
        )

    method_gdf = method_gdf.rename(columns={ffh_field: "floor_height_m"})
    # Make method input column names lower case and remove special characters
    method_gdf.columns = method_gdf.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )
    method_gdf["id"] = [uuid.uuid4() for _ in range(len(method_gdf.index))]
    method_gdf = method_gdf.set_index(["id"])

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()
        click.echo("Copying validation points to PostgreSQL...")
        method_gdf.to_postgis(
            "temp_method",
            conn,
            schema="public",
            if_exists="replace",
            index=True,
            dtype={"id": UUID, "floor_height_m": Numeric},
        )
        temp_method = Table("temp_method", Base.metadata, autoload_with=conn)

        if input_cadastre:
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(Path(input_cadastre).name, error)

            click.echo("Copying cadastre to PostgreSQL...")
            cadastre_gdf.to_postgis(
                "temp_cadastre",
                conn,
                schema="public",
                if_exists="replace",
                index=True,
                index_label="id",
            )
            temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=conn)

        if flatten_cadastre:
            click.echo("Flattening cadastre geometries...")
            temp_cadastre = etl.flatten_cadastre_geoms(
                session, conn, Base, temp_cadastre
            )

        step_count_id = etl.get_or_create_method_id(session, "Step counting")
        survey_id = etl.get_or_create_method_id(session, "Surveyed")

        click.echo("Inserting records into floor_measure table...")
        # Build select queries that will be inserted into the floor_measure table,
        # we do this for both surveyed and step counting methods
        # First, by point-building intersection
        step_count_query_intersects = etl.build_floor_measure_query(
            temp_method,
            "floor_height_m",
            step_count_id,
            accuracy_measure=60,
            storey=0,
            join_by="intersects",
            step_counting=True,
            step_size=step_size,
        )
        step_count_intersects_ids = etl.insert_floor_measure(
            session, step_count_query_intersects
        )
        survey_query_intersects = etl.build_floor_measure_query(
            temp_method,
            "floor_height_m",
            survey_id,
            accuracy_measure=90,
            storey=0,
            join_by="intersects",
            step_counting=False,
            step_size=step_size,
        )
        survey_intersects_ids = etl.insert_floor_measure(
            session, survey_query_intersects
        )
        validation_inserted_ids = step_count_intersects_ids + survey_intersects_ids

        if input_cadastre:
            # Second, by joining to buildings with a common cadastre lot
            step_count_query_cadastre_knn = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                step_count_id,
                accuracy_measure=60,
                storey=0,
                join_by="cadastre",
                step_counting=True,
                step_size=step_size,
                cadastre=temp_cadastre,
            )
            survey_query_cadastre_knn = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                survey_id,
                accuracy_measure=90,
                storey=0,
                join_by="cadastre",
                step_counting=False,
                step_size=step_size,
                cadastre=temp_cadastre,
            )

            if join_largest:
                click.echo("Joining with largest building on lot...")
                # Modify select to join to largest building on the lot for distinct points
                step_count_query_cadastre_knn = step_count_query_cadastre_knn.order_by(
                    temp_method.c.id, func.ST_Area(Building.outline).desc()
                ).distinct(temp_method.c.id)
                survey_query_cadastre_knn = survey_query_cadastre_knn.order_by(
                    temp_method.c.id, func.ST_Area(Building.outline).desc()
                ).distinct(temp_method.c.id)
            step_count_cadastre_knn_ids = etl.insert_floor_measure(
                session, step_count_query_cadastre_knn
            )
            survey_cadastre_knn_ids = etl.insert_floor_measure(
                session, survey_query_cadastre_knn
            )

            # Third, by nearest-neighbour outside the extents of the cadastre
            step_count_query_knn = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                step_count_id,
                accuracy_measure=60,
                storey=0,
                join_by="knn",
                step_counting=True,
                step_size=step_size,
                cadastre=temp_cadastre,
            )
            step_count_knn_ids = etl.insert_floor_measure(session, step_count_query_knn)
            step_count_query_knn = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                survey_id,
                accuracy_measure=60,
                storey=0,
                join_by="knn",
                cadastre=temp_cadastre,
                step_counting=False,
                step_size=step_size,
            )
            survey_knn_ids = etl.insert_floor_measure(session, step_count_query_knn)

            validation_inserted_ids += (
                step_count_cadastre_knn_ids
                + survey_cadastre_knn_ids
                + step_count_knn_ids
                + survey_knn_ids
            )

            # List of tuples to list of ids
            validation_inserted_ids = [id for (id,) in validation_inserted_ids]

        if validation_inserted_ids:
            if not dataset_name:
                dataset_name = Path(input_data).name

            nexis_dataset_id = etl.get_or_create_dataset_id(
                session, dataset_name, dataset_desc, dataset_src
            )
            etl.insert_floor_measure_dataset_association(
                session, nexis_dataset_id, validation_inserted_ids
            )

        temp_method.drop(conn)
        if input_cadastre:
            temp_cadastre.drop(conn)

        click.echo("Validation ingestion complete")


@click.command()
@click.option("-i", "--input-json", required=True, type=click.File(), help="Input main methodology floor height JSON file path.")  # fmt: skip
@click.option("--ffh-field", type=str, required=True, help="Name of the first floor height field in the input JSON.")  # fmt: skip
@click.option("--method-name", default="Main Methodology", type=str, help="The floor measure method name.")  # fmt: skip
@click.option("--dataset-name", type=str, help="The floor measure dataset name.")  # fmt: skip
@click.option("--dataset-desc", default="Main methodology output - LIDAR", show_default=True, type=str, help="The floor measure dataset description.")  # fmt: skip
@click.option("--dataset-src", default="FrontierSI", show_default=True, type=str, help="The floor measure dataset source.")  # fmt: skip
def ingest_main_method_measures(
    input_json: click.File,
    ffh_field: str,
    method_name: str,
    dataset_name: str,
    dataset_desc: str,
    dataset_src: str,
):
    """Ingest main methodology floor height JSON"""
    click.secho("Ingesting Main Methodology measures", bold=True)
    try:
        click.echo("Loading Floor Height JSON...")
        json_data = json.load(input_json)
        method_df = pd.DataFrame(json_data["buildings"])
    except Exception as error:
        raise click.exceptions.FileError(input_json.name, error)

    method_df = method_df.rename(
        columns={
            "id": "building_id",
            ffh_field: "height",
        }
    )

    method_df = method_df[~method_df["height"].isna()]

    # TODO: remove these constants when these fields are populated in the JSON
    method_df["accuracy_measure"] = 0
    method_df["storey"] = 0

    # Make method input column names lower case and remove special characters
    method_df.columns = method_df.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )
    method_df["id"] = [uuid.uuid4() for _ in range(len(method_df.index))]
    method_df = method_df.set_index(["id"])

    # Create aux_info json column
    aux_info_df = method_df.drop(
        columns=["building_id", "height", "storey", "accuracy_measure"], axis=1
    ).copy()
    aux_info_df = aux_info_df.replace(np.nan, None)

    method_df["aux_info"] = aux_info_df.apply(
        lambda row: json.dumps(row.to_dict()), axis=1
    )
    method_df = method_df.drop(columns=aux_info_df.columns, axis=1)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        click.echo("Inserting records into floor_measure table...")

        method_id = etl.get_or_create_method_id(session, method_name)
        method_dataset_id = etl.get_or_create_dataset_id(
            session, dataset_name, dataset_desc, dataset_src
        )

        method_df["method_id"] = method_id

        try:
            method_df.to_sql(
                "floor_measure",
                conn,
                schema="public",
                if_exists="append",
                index=True,
                dtype={
                    "id": UUID,
                    "building_id": UUID,
                    "method_id": UUID,
                    "height": Numeric,
                    "aux_info": JSON,
                },
                method=etl.psql_insert_copy,
            )
        except psycopg2.errors.ForeignKeyViolation:
            raise click.UsageError(
                "The ingest-main-method-measures command can only be used after "
                "ingesting buildings, also ensure that the building IDs match those "
                "in the input JSON."
            )

        etl.insert_floor_measure_dataset_association(
            session, method_dataset_id, method_df.index.tolist()
        )

    click.echo("Main methodology ingestion complete")


@click.command()
@click.option("--pano-path", type=click.Path(), help="Path to folder containing panorama images.")  # fmt: skip
@click.option("--lidar-path", type=click.Path(), help="Path to folder containing LIDAR images.")  # fmt: skip
@click.option("--dataset-name", type=str, default="Main Methodology", help="The floor measure dataset name to attach images to.")  # fmt: skip
def ingest_main_method_images(
    pano_path: click.Path, lidar_path: click.Path, dataset_name: str
):
    """Ingest main methodology images"""
    if not pano_path and not lidar_path:
        raise click.UsageError(
            "Either --pano-path or --lidar-path must be provided to ingest images."
        )

    def image_to_bytearray(image_path):
        with open(image_path, "rb") as f:
            return f.read()

    click.secho("Ingesting Main Methodology images", bold=True)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        click.echo("Selecting records from floor_measure table...")

        measure_df = etl.get_measure_image_names(conn, dataset_name)

        if measure_df.empty:
            raise click.UsageError(
                "The ingest-main-method-images command can only be used after "
                "ingesting the main methdology floor measures."
            )

        # TODO Implement lidar image ingestion

        # Ingest panorama images
        if pano_path:
            click.echo("Ingesting panorama images...")
            # Associate Method IDs with panorama image paths
            pano_df = measure_df[["frame_filename"]].copy()

            # Get pano filenames by globbing the pano_path
            pano_df["pano_path"] = pano_df.frame_filename.apply(
                lambda filename: list(Path(pano_path).glob(f"{Path(filename).stem}*"))
            )
            # Normalise so that each row contains one filepath
            pano_df = pano_df.explode("pano_path")
            pano_df = pano_df[pano_df["pano_path"].notna()]

            # Add ID and additional fields
            pano_df["id"] = [uuid.uuid4() for _ in range(len(pano_df.index))]
            pano_df = pano_df.set_index(["id"], drop=True)
            pano_df["filename"] = pano_df.pano_path.apply(lambda path: Path(path).name)
            pano_df["type"] = "panorama"

            # Join the floor_measure_ids
            pano_df = pano_df.join(
                measure_df[["id", "frame_filename"]].set_index("frame_filename"),
                on="frame_filename",
            )
            pano_df = pano_df.rename(columns={"id": "floor_measure_id"})

            # Create byte arrays of the images
            pano_df["image_data"] = pano_df.pano_path.apply(
                lambda filename: image_to_bytearray(filename)
            )

            pano_df = pano_df.drop(columns=["frame_filename", "pano_path"], axis=1)

            pano_df.to_sql(
                "floor_measure_image",
                conn,
                schema="public",
                if_exists="append",
                index=True,
                dtype={
                    "id": UUID,
                    "image_data": LargeBinary,
                    "floor_measure_id": UUID,
                    "type": String,
                },
            )

        if lidar_path:
            raise NotImplementedError

    click.echo("Image ingestion complete")


@click.command()
@click.option("-o", "--output-file", required=True, type=str, help="Output OGR dataset file path.")  # fmt: skip
@click.option("--normalise-aux-info", is_flag=True, help="Normalise the aux_info field into separate columns.")  # fmt: skip
@click.option("--buildings-only", is_flag=True, help="Export buildings only, for input to object detection processing.")  # fmt: skip
def export_ogr_file(output_file: str, normalise_aux_info: bool, buildings_only: bool):
    """Export an OGR file of the data model"""
    click.secho("Exporting OGR file", bold=True)
    if buildings_only:
        select_query = etl.build_buildings_query()
    else:
        select_query = etl.build_denormalised_query()

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        try:
            output_file = Path(output_file)
            click.echo(f"Writing OGR file: {output_file}")
            etl.write_ogr_file(
                output_file, select_query, conn, normalise_aux_info, buildings_only
            )
        except Exception as error:
            raise click.exceptions.FileError(Path(output_file).name, error)
    click.echo("Export complete")


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)
cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)
cli.add_command(join_address_buildings)
cli.add_command(ingest_nexis_measures)
cli.add_command(ingest_validation_measures)
cli.add_command(ingest_main_method_measures)
cli.add_command(ingest_main_method_images)
cli.add_command(export_ogr_file)


if __name__ == "__main__":
    cli(max_content_width=250)


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli(max_content_width=250)
