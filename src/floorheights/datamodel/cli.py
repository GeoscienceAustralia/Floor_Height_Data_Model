import json
import uuid
from pathlib import Path

import boto3
import click
import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
import rasterio
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
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
@click.option("-i", "--input-address", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to address points file.")  # fmt: skip
@click.option("-c", "--chunksize", type=int, default=None, help="Number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
def ingest_address_points(input_address: click.Path, chunksize: int):
    """Ingest address points

    Takes an input address points file and ingests it into the data model.
    """
    click.secho("Ingesting address points", bold=True)
    try:
        click.echo("Loading address points...")
        address = etl.read_ogr_file(
            input_address,
            columns=[
                "ADDRESS_DETAIL_PID",
                "COMPLETE_ADDRESS",
                "GEOCODE_TYPE",
                "PRIMARY_SECONDARY",
            ],
        )
    except Exception as error:
        raise click.exceptions.FileError(input_address, error)

    address = address[
        (address.GEOCODE_TYPE == "BUILDING CENTROID")
        | (address.GEOCODE_TYPE == "PROPERTY CENTROID")
    ]
    address = address.rename(
        columns={
            "COMPLETE_ADDRESS": "address",
            "ADDRESS_DETAIL_PID": "gnaf_id",
            "GEOCODE_TYPE": "geocode_type",
            "PRIMARY_SECONDARY": "primary_secondary",
        }
    )
    address = address.rename_geometry("location")

    # Generate UUIDs based on the GNAF IDs
    click.echo("Generating UUIDs...")
    address["id"] = address["gnaf_id"].apply(etl.generate_uuid)
    address = address.set_index("id")

    click.echo("Copying to PostgreSQL...")
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        address.to_postgis(
            "address_point",
            conn,
            schema="public",
            if_exists="append",
            index=True,
            chunksize=chunksize,
            dtype={"id": UUID},
        )
        click.echo("Address ingestion complete")


@click.command()
@click.option("-i", "--input-buildings", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to building footprints file.")  # fmt: skip
@click.option("-d", "--input-dem", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=False), help="Path to DEM file, used to sample building footprint ground height.")  # fmt: skip
@click.option("-s", "--chunksize", type=int, default=None, help="Number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
@click.option("--split-by-cadastre", type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Split buildings by cadastre, specify path to cadastre vector file for splitting.")  # fmt: skip
@click.option("--join-land-zoning", type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Join land zoning type to buildings, specify path to land zoning vector file for sampling.")  # fmt: skip
@click.option("--land-zoning-field", type=str, help="Name of the land zoning dataset's field to sample with buildings.")  # fmt: skip
@click.option("--remove-small", type=float, is_flag=False, flag_value=30, default=None, help="Remove smaller buildings, optionally specify an area threshold in square metres.  [default: 30.0]")  # fmt: skip
@click.option("--remove-overlapping", type=float, is_flag=True, flag_value=0.80, default=None, help="Remove overlapping buildings, optionally specify an intersection ratio threshold.  [default: 0.80]")  # fmt: skip
def ingest_buildings(
    input_buildings: click.Path,
    input_dem: click.Path,
    chunksize: int,
    split_by_cadastre: click.Path,
    join_land_zoning: click.Path,
    land_zoning_field: str,
    remove_small: float,
    remove_overlapping: float,
):
    """Ingest building footprints

    Takes an input building footprints polygon file and ingests it into the data model.
    Requires an input DEM file to sample the building footprint ground height.
    """
    if join_land_zoning and not land_zoning_field:
        raise click.UsageError(
            "--join-land-zoning must be used with --land-zoning-field"
        )

    click.secho("Ingesting building footprints", bold=True)
    try:
        click.echo("Loading DEM...")
        dem = rasterio.open(input_dem)
        dem_crs = dem.crs
    except Exception as error:
        raise click.exceptions.FileError(input_dem, error)

    click.echo("Creating mask...")
    bounds = dem.bounds
    mask_geom = box(*bounds)
    mask_df = gpd.GeoDataFrame(
        {"id": 1, "geometry": [mask_geom]}, crs=dem_crs.to_string()
    )
    # Transform mask to GDA2020
    mask_df = mask_df.to_crs(7844)
    mask_bbox = tuple(map(float, mask_df.total_bounds))

    click.echo("Loading building footprints...")
    try:
        buildings = etl.read_ogr_file(
            input_buildings, columns=["geometry"], bbox=mask_bbox
        )
    except Exception as error:
        raise click.exceptions.FileError(input_buildings, error)

    buildings = buildings.explode()
    buildings = buildings.to_crs(
        dem_crs.to_epsg()
    )  # Transform buildings to CRS of our DEM

    if split_by_cadastre:
        click.echo("Splitting buildings by cadastre...")
        try:
            cadastre = etl.read_ogr_file(split_by_cadastre, columns=["geometry"])
            cadastre = cadastre.to_crs(dem.crs)
        except Exception as error:
            raise click.exceptions.FileError(split_by_cadastre, error)

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
                if not ~address_points.within(mask_df.geometry.iloc[0]).all():
                    raise Exception

            except Exception:
                raise click.UsageError(
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
            raise click.exceptions.FileError(join_land_zoning, error)

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
    buildings = buildings.to_crs(7844)  # Transform back to GDA2020
    buildings = buildings.rename_geometry("outline")

    # Drop duplicate geometries
    buildings["outline"] = buildings.normalize()
    buildings = buildings.drop_duplicates(subset="outline")

    # Generate UUIDs based on the building geometries
    click.echo("Generating UUIDs...")
    buildings["id"] = buildings["outline"].apply(etl.generate_uuid)
    buildings = buildings.set_index("id")

    click.echo("Copying to PostgreSQL...")
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        buildings.to_postgis(
            "building",
            conn,
            schema="public",
            if_exists="append",
            index=True,
            chunksize=chunksize,
            dtype={"id": UUID},
        )

        if remove_overlapping:
            click.echo("Removing overlapping buildings...")
            result = etl.remove_overlapping_geoms(
                session, remove_overlapping, bbox=mask_bbox
            )
            click.echo(f"Removed {result.rowcount} overlapping buildings...")

        click.echo("Building ingestion complete")


@click.command()
@click.option("-c", "--input-cadastre", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to cadastre vector dataset to support address joining.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. May reduce false matches.")  # fmt: skip
def join_address_buildings(input_cadastre: click.Path, flatten_cadastre: bool):
    """Join address points to building outlines

    Requires an input cadastre vector file to support joining.

    \b
    The join is performed in two steps:
    1. Match addresses geocoded to building centroids using a KNN join with a maximum distance of 5m (to account for inaccuracies of building footprints).
    2. Match addresses geocoded to property centroids by joining to buildings sharing a common parcel.
    """
    click.secho("Joining addresses to buildings", bold=True)
    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()
        click.echo("Loading cadastre...")
        try:
            cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
        except Exception as error:
            raise click.exceptions.FileError(input_cadastre, error)

        cadastre_bbox = tuple(map(float, cadastre_gdf.total_bounds))
        cadastre_gdf = cadastre_gdf.explode()

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

        click.echo("Performing join for building centroid addresses...")
        # Match for addresses geocoded to building centroids using KNN max distance 5m,
        # to account for inaccuracies of building footprints
        select_query = etl.build_address_match_query(
            join_by="knn",
            geocode_type="BUILDING CENTROID",
            knn_max_distance=5,
            bbox=cadastre_bbox,
        )
        etl.insert_address_building_association(session, select_query)

        click.echo("Performing join for property centroid addresses...")
        # Create base query for joining property centroid addresses
        select_query = etl.build_address_match_query(
            join_by="cadastre",
            geocode_type="PROPERTY CENTROID",
            cadastre=temp_cadastre,
            skip_matched_buildings=False,
            bbox=cadastre_bbox,
        )

        # Modify base query to join to largest building on the parcel for distinct
        # non-primary and non-secondary address (i.e. non-strata addresses)
        select_query_non_strata = (
            select_query.where(
                AddressPoint.primary_secondary == None,  # noqa: E711
            )
            .order_by(AddressPoint.id, func.ST_Area(Building.outline).desc())
            .distinct(AddressPoint.id)
        )
        etl.insert_address_building_association(session, select_query_non_strata)

        # Modify base query to join to all buildings on the parcel for primary addresses
        # (i.e. strata addresses)
        select_query_strata = select_query.where(
            AddressPoint.primary_secondary == "PRIMARY",
        )
        etl.insert_address_building_association(session, select_query_strata)

        # Finally, join by intersection for property centroid, secondary addresses
        # (i.e. strata addresses that intersect a building)
        select_query_strata_secondary = etl.build_address_match_query(
            join_by="intersects",
            geocode_type="PROPERTY CENTROID",
            skip_matched_buildings=True,
            bbox=cadastre_bbox,
        ).where(
            AddressPoint.primary_secondary == "SECONDARY",
        )
        etl.insert_address_building_association(session, select_query_strata_secondary)

        temp_cadastre.drop(conn)

    click.echo("Joining complete")


@click.command()
@click.option("-i", "--input-nexis", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to NEXIS CSV file.")  # fmt: skip
@click.option("--confidence-field", type=str, default=None, help="Name of the first floor height confidence field.")  # fmt: skip
@click.option("-c", "--input-cadastre", type=click.Path(exists=True, file_okay=True, dir_okay=True), default=None, help="Path to cadastre vector dataset to support joining non-GNAF NEXIS points.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. May reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measure points to the largest building on the parcel. May reduce the number of false matches to non-dwellings.")  # fmt: skip
def ingest_nexis_measures(
    input_nexis: click.Path,
    confidence_field: str,
    input_cadastre: click.Path,
    flatten_cadastre: bool,
    join_largest: bool,
):
    """Ingest NEXIS floor height measures

    Takes a CSV file containing NEXIS floor measures and ingests it into the data model.
    Assumes the NEXIS points are in GDA1994 (EPSG:4283).

    \b
    The NEXIS points are joined to buildings in two steps:
    1. Match to buildings that share a common GNAF ID.
    2. Match to buildings for Non-GNAF IDs by point-building intersection, then by common cadastral parcel (if provided).
    """
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

    if confidence_field is not None and confidence_field not in nexis_gdf.columns:
        raise click.exceptions.BadParameter(
            f"Field '{confidence_field}' not found in input NEXIS file"
        )
    if confidence_field is not None:
        nexis_gdf = nexis_gdf.rename(columns={confidence_field: "confidence"})
    else:
        nexis_gdf["confidence"] = None

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()

        if input_cadastre:
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(input_cadastre, error)
            # Clip NEXIS points to cadastre extent
            nexis_gdf = gpd.clip(nexis_gdf, cadastre_gdf)
        else:
            # Subset NEXIS points based GNAF IDs in the database
            gnaf_ids = session.execute(select(AddressPoint.gnaf_id)).all()
            gnaf_ids = [row[0] for row in gnaf_ids]
            nexis_gdf = nexis_gdf[nexis_gdf["lid"].isin(gnaf_ids)]

        nexis_gdf["id"] = [uuid.uuid4() for _ in range(len(nexis_gdf.index))]
        nexis_gdf = nexis_gdf.set_index(["id"])
        nexis_gdf = nexis_gdf.rename_geometry("location")

        click.echo("Copying NEXIS points to PostgreSQL...")
        nexis_gdf.to_postgis(
            "temp_nexis",
            conn,
            schema="public",
            if_exists="replace",
            index=True,
            dtype={"id": UUID, "floor_height_m": Numeric, "confidence": Numeric},
        )
        temp_nexis = Table("temp_nexis", Base.metadata, autoload_with=conn)

        # Build select query to insert into the floor_measure table for GNAF ID matches
        method_id = etl.get_or_create_method_id(session, "Random Sampling")
        modelled_query_gnaf = etl.build_floor_measure_query(
            temp_nexis,
            "floor_height_m",
            method_id,
            "confidence",
            storey=0,
            join_by="gnaf_id",
            gnaf_id_col="lid",
        )

        click.echo("Inserting GNAF records into floor_measure table...")
        modelled_inserted_ids = etl.insert_floor_measure(session, modelled_query_gnaf)

        click.echo("Inserting non-GNAF records into floor_measure table...")
        # Build select queries to insert into the floor_measure table for non-GNAF addresses
        # First, by point-building intersection
        modelled_query_intersect = etl.build_floor_measure_query(
            temp_nexis,
            "floor_height_m",
            method_id,
            "confidence",
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

            # Second, by joining to buildings with a common cadastre parcel
            modelled_query_cadastre = etl.build_floor_measure_query(
                temp_nexis,
                "floor_height_m",
                method_id,
                "confidence",
                storey=0,
                join_by="cadastre",
                cadastre=temp_cadastre,
            ).where(temp_nexis.c.lid.notlike("GA%"))

            if join_largest:
                click.echo("Joining with largest building on parcel...")
                # Modify select to join to largest building on the parcel for distinct points
                modelled_query_cadastre = modelled_query_cadastre.order_by(
                    temp_nexis.c.id, func.ST_Area(Building.outline).desc()
                ).distinct(temp_nexis.c.id)
            modelled_inserted_cadastre_ids = etl.insert_floor_measure(
                session, modelled_query_cadastre
            )

            # Concat the inserted id lists
            modelled_inserted_ids += modelled_inserted_cadastre_ids

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
@click.option("-i", "--input-data", required=True, type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to input validation points dataset.")  # fmt: skip
@click.option("--ffh-field", type=str, required=True, help="Name of the first floor height field.")  # fmt: skip
@click.option("--confidence-field", type=str, default=None, help="Name of the first floor height confidence field.")  # fmt: skip
@click.option("--step-size", type=float, is_flag=False, flag_value=0.28, default=None, help="Optional step size value in metres, if used it will separate measures into 'Step counting' and 'Surveyed' methods. [default: 0.28]")  # fmt: skip
@click.option("-c", "--input-cadastre", type=click.Path(exists=True, file_okay=True, dir_okay=True), help="Path to cadastre vector dataset to support joining measures to buildings.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. May reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measures to the largest building on the parcel. May reduce the number of false matches to non-dwellings.")  # fmt: skip
@click.option("--method-name", type=str, default="Surveyed", help="Name of the floor measure method.")  # fmt: skip
@click.option("--dataset-name", type=str, default="Validation", show_default=True, help="Name of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-desc", type=str, help="Description of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-src", type=str, help="Source of the floor measure dataset.")  # fmt: skip
def ingest_validation_measures(
    input_data: click.Path,
    ffh_field: str,
    confidence_field: str,
    step_size: float,
    input_cadastre: click.Path,
    flatten_cadastre: bool,
    join_largest: bool,
    method_name: str,
    dataset_name: str,
    dataset_desc: str,
    dataset_src: str,
):
    """Ingest validation floor height measures

    Takes a points file of validation floor measures (e.g. Council provided) and ingests
    it into the data model. Requires the field name for the first floor height.
    """
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
        raise click.exceptions.FileError(input_data, error)

    if ffh_field not in method_gdf.columns:
        raise click.exceptions.BadParameter(
            f"Field '{ffh_field}' not found in input validation points file"
        )

    if confidence_field is not None and confidence_field not in method_gdf.columns:
        raise click.exceptions.BadParameter(
            f"Field '{confidence_field}' not found in input NEXIS file"
        )
    if confidence_field is not None:
        method_gdf = method_gdf.rename(columns={confidence_field: "confidence"})
    else:
        method_gdf["confidence"] = None

    method_gdf = method_gdf.rename(columns={ffh_field: "floor_height_m"})
    method_gdf = method_gdf.rename_geometry("location")
    method_gdf = method_gdf.dropna(subset=["floor_height_m"])
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
            dtype={"id": UUID, "floor_height_m": Numeric, "confidence": Numeric},
        )
        temp_method = Table("temp_method", Base.metadata, autoload_with=conn)

        if input_cadastre:
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(input_cadastre, error)

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

        # If no step size is provided, we ingest all measures as a single method
        if step_size is None:
            method_id = etl.get_or_create_method_id(session, method_name)

            click.echo("Inserting validation measures into floor_measure table...")
            # First, join by point-building intersection
            click.echo("Joining by intersection...")
            survey_intersects_query = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                method_id,
                "confidence",
                storey=0,
                join_by="intersects",
                step_counting=False,
                step_size=None,
            )
            survey_intersects_ids = etl.insert_floor_measure(
                session, survey_intersects_query
            )

            validation_ids = survey_intersects_ids

            if input_cadastre:
                click.echo("Joining by cadastre...")
                # Second, join to buildings with a common cadastre parcel
                survey_cadastre_query = etl.build_floor_measure_query(
                    temp_method,
                    "floor_height_m",
                    method_id,
                    "confidence",
                    storey=0,
                    join_by="cadastre",
                    step_counting=False,
                    step_size=None,
                    cadastre=temp_cadastre,
                )

                if join_largest:
                    click.echo("Joining with largest building on parcel...")
                    # Modify select to join to largest building on the parcel for distinct points
                    survey_query_cadastre = survey_cadastre_query.order_by(
                        temp_method.c.id, func.ST_Area(Building.outline).desc()
                    ).distinct(temp_method.c.id)

                survey_cadastre_ids = etl.insert_floor_measure(
                    session, survey_query_cadastre
                )

                validation_ids += survey_cadastre_ids

        # If step size is provided, separate measures into surveyed and step counting
        else:
            click.echo(
                "Inserting surveyed & step counted measures into floor_measure table..."
            )

            survey_id = etl.get_or_create_method_id(session, "Surveyed")
            step_count_id = (
                etl.get_or_create_method_id(session, "Step counting")
                if step_size
                else None
            )
            # Join by point-building intersection
            click.echo("Joining by intersection...")
            step_count_intersects_query = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                step_count_id,
                "confidence",
                storey=0,
                join_by="intersects",
                step_counting=True,
                step_size=step_size,
            )
            step_count_intersects_ids = etl.insert_floor_measure(
                session, step_count_intersects_query
            )
            survey_intersects_query = etl.build_floor_measure_query(
                temp_method,
                "floor_height_m",
                survey_id,
                "confidence",
                storey=0,
                join_by="intersects",
                step_counting=False,
                step_size=step_size,
            )
            survey_intersects_ids = etl.insert_floor_measure(
                session, survey_intersects_query
            )
            validation_ids = step_count_intersects_ids + survey_intersects_ids

            if input_cadastre:
                click.echo("Joining by cadastre...")
                # Join to buildings with a common cadastre parcel
                step_count_cadastre_query = etl.build_floor_measure_query(
                    temp_method,
                    "floor_height_m",
                    step_count_id,
                    "confidence",
                    storey=0,
                    join_by="cadastre",
                    step_counting=True,
                    step_size=step_size,
                    cadastre=temp_cadastre,
                )
                survey_cadastre_query = etl.build_floor_measure_query(
                    temp_method,
                    "floor_height_m",
                    survey_id,
                    "confidence",
                    storey=0,
                    join_by="cadastre",
                    step_counting=False,
                    step_size=step_size,
                    cadastre=temp_cadastre,
                )

                if join_largest:
                    click.echo("Joining with largest building on parcel...")
                    # Modify select to join to largest building on the parcel for distinct points
                    step_count_query_cadastre = step_count_cadastre_query.order_by(
                        temp_method.c.id, func.ST_Area(Building.outline).desc()
                    ).distinct(temp_method.c.id)
                    survey_query_cadastre = survey_cadastre_query.order_by(
                        temp_method.c.id, func.ST_Area(Building.outline).desc()
                    ).distinct(temp_method.c.id)

                step_count_cadastre_ids = etl.insert_floor_measure(
                    session, step_count_query_cadastre
                )
                survey_cadastre_ids = etl.insert_floor_measure(
                    session, survey_query_cadastre
                )

                validation_ids += step_count_cadastre_ids + survey_cadastre_ids

        # List of tuples to list of ids
        validation_ids = [id for (id,) in validation_ids]

        if validation_ids:
            if not dataset_name:
                dataset_name = input_data

            dataset_id = etl.get_or_create_dataset_id(
                session, dataset_name, dataset_desc, dataset_src
            )
            etl.insert_floor_measure_dataset_association(
                session, dataset_id, validation_ids
            )

        temp_method.drop(conn)
        if input_cadastre:
            temp_cadastre.drop(conn)

        click.echo("Validation ingestion complete")


@click.command()
@click.option("-i", "--input-file", required=True, type=click.Path(file_okay=True, dir_okay=False), help="Path to parquet file containing main methodology measures.")  # fmt: skip
@click.option("--dataset-name", type=str, default="FFH Model Output", help="Name of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-desc", type=str, default="Outputs from the FFH processing model", show_default=True, help="Name of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-src", type=str, default="FrontierSI", show_default=True, help="Source of the floor measure dataset.")  # fmt: skip
def ingest_main_method_measures(
    input_file: click.Path,
    dataset_name: str,
    dataset_desc: str,
    dataset_src: str,
):
    """Ingest main methodology from floor height parquet

    Takes a parquet file output from the processing workflow and ingests Main
    Methodology measures into the data model.
    """
    click.secho("Ingesting Main Methodology measures", bold=True)
    try:
        click.echo("Loading Floor Height parquet...")
        method_df = pd.read_parquet(input_file)
    except Exception as error:
        raise click.exceptions.FileError(input_file, error)

    # Drop original geometry, set to location of door
    method_df = method_df.drop(columns=["geometry"])
    method_gdf = gpd.GeoDataFrame(
        method_df,
        geometry=gpd.points_from_xy(
            method_df["door_lon"], method_df["door_lat"], crs="EPSG:7844"
        ),
    )
    method_gdf = method_gdf.rename_geometry("location")

    method_gdf["storey"] = 0

    # Cast building_id strings to UUIDs
    method_gdf["building_id"] = method_gdf["building_id"].apply(uuid.UUID)

    # Make method input column names lower case and remove special characters
    method_gdf.columns = method_gdf.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )

    # Deserialise bboxes as a list of dicts
    method_gdf["bboxes"] = method_gdf["bboxes"].apply(lambda row: json.loads(row))

    # Create aux_info json column
    aux_info_df = method_gdf.drop(
        columns=[
            "building_id",
            "storey",
            "location",
        ],
        axis=1,
    ).copy()
    aux_info_df = aux_info_df.replace(np.nan, None)
    method_gdf["aux_info"] = aux_info_df.apply(
        lambda row: json.dumps(row.to_dict()), axis=1
    )
    # method_gdf = method_gdf.drop(columns=aux_info_df.columns, axis=1)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        click.echo("Inserting records into floor_measure table...")

        methods = {
            "ffh1": "Main Method - FFH1",
            "ffh2": "Main Method - FFH2",
            "ffh3": "Main Method - FFH3",
        }

        # Iterate methods from the processing output and ingest measures
        for method_field, method_name in methods.items():
            # Filter method_gdf for the current method
            method_gdf_filtered = method_gdf[method_gdf[method_field].notna()].copy()
            method_gdf_filtered["height"] = method_gdf_filtered[method_field]

            if method_gdf_filtered.empty:
                click.echo(f"No records found for {method_field}, skipping...")
                continue

            method_id = etl.get_or_create_method_id(session, method_name)
            method_dataset_id = etl.get_or_create_dataset_id(
                session, dataset_name, dataset_desc, dataset_src
            )

            method_gdf_filtered["method_id"] = method_id

            # Create UUID index
            method_gdf_filtered["id"] = [
                uuid.uuid4() for _ in range(len(method_gdf_filtered.index))
            ]
            method_gdf_filtered = method_gdf_filtered.set_index(["id"])

            try:
                method_gdf_filtered.drop(
                    columns=aux_info_df.columns, axis=1
                ).to_postgis(
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
                )
            except psycopg2.errors.ForeignKeyViolation:
                raise click.UsageError(
                    "The ingest-main-method-measures command can only be used after "
                    "ingesting buildings, also ensure that the building IDs match those "
                    "in the input JSON."
                )

            etl.insert_floor_measure_dataset_association(
                session, method_dataset_id, method_gdf_filtered.index.tolist()
            )

    click.echo("Main methodology ingestion complete")


@click.command()
@click.option("-i", "--input-file", required=True, type=click.Path(file_okay=True, dir_okay=False), help="Path to parquet file containing gap fill measures.")  # fmt: skip
@click.option("--ffh-field", type=str, default="ensemble_ffh", help="Name of the first floor height field in the input parquet.")  # fmt: skip
@click.option("--confidence-field", type=str, default="ensemble_confidence", help="Name of the first floor height confidence field.")  # fmt: skip
@click.option("--method-name",  type=str, default="Main Method - Ensemble", help="Name of the floor measure method.")  # fmt: skip
@click.option("--dataset-name", type=str, default="FFH Model Output", help="Name of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-desc", type=str, default="Outputs from the FFH processing model", show_default=True, help="Name of the floor measure dataset.")  # fmt: skip
@click.option("--dataset-src", type=str, default="FrontierSI", show_default=True, help="Source of the floor measure dataset.")  # fmt: skip
def ingest_gap_fill_measures(
    input_file: click.Path,
    ffh_field: str,
    confidence_field: str,
    method_name: str,
    dataset_name: str,
    dataset_desc: str,
    dataset_src: str,
):
    """Ingest gap fill measures from floor height parquet

    Takes a parquet file output from the processing workflow and ingests Gap Fill
    (ensemble) measures into the data model.
    """
    click.secho("Ingesting Gap Fill measures", bold=True)
    try:
        click.echo("Loading Floor Height parquet...")
        method_df = pd.read_parquet(input_file)
    except Exception as error:
        raise click.exceptions.FileError(input_file, error)

    if confidence_field is not None and confidence_field not in method_df.columns:
        raise click.exceptions.BadParameter(
            f"Field '{confidence_field}' not found in input parquet file"
        )
    if confidence_field is not None:
        method_df = method_df.rename(columns={confidence_field: "confidence"})
    else:
        method_df["confidence"] = None

    method_df = method_df.drop(columns=["geometry"])
    method_df["height"] = method_df[ffh_field]
    method_df = method_df[~method_df["height"].isna()]
    method_df["storey"] = 0

    # Cast building_id strings to UUIDs
    method_df["building_id"] = method_df["building_id"].apply(uuid.UUID)

    # Make method input column names lower case and remove special characters
    method_df.columns = method_df.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )

    # Deserialise bboxes as a list of dicts
    method_df["bboxes"] = method_df["bboxes"].apply(lambda row: json.loads(row))

    # Create aux_info json column
    aux_info_df = method_df.drop(
        columns=[
            "building_id",
            "height",
            "storey",
            "confidence",
        ],
        axis=1,
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

        # Create UUID index
        method_df["id"] = [uuid.uuid4() for _ in range(len(method_df.index))]
        method_df = method_df.set_index(["id"])

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
                "The ingest-gap-fill-measures command can only be used after "
                "ingesting buildings, also ensure that the building IDs match those "
                "in the input parquet."
            )

        etl.insert_floor_measure_dataset_association(
            session, method_dataset_id, method_df.index.tolist()
        )

    click.echo("Gap Fill ingestion complete")


@click.command()
@click.option("--pano-path", type=click.Path(exists=True), help="Path to folder containing panorama images.")  # fmt: skip
@click.option("--lidar-path", type=click.Path(exists=True), help="Path to folder containing LIDAR images.")  # fmt: skip
def ingest_main_method_images(pano_path: click.Path, lidar_path: click.Path):
    """Ingest main methodology images

    Takes a path to Panorama and/or LIDAR images ingests them into the data model.
    """
    if not pano_path and not lidar_path:
        raise click.UsageError(
            "Either --pano-path or --lidar-path must be provided to ingest images."
        )

    def image_to_bytearray(image_path):
        try:
            with open(image_path, "rb") as f:
                return f.read()
        except FileNotFoundError as error:
            click.echo(f"Skipping image: {error}")

    click.secho("Ingesting Main Methodology images", bold=True)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        click.echo("Selecting records from floor_measure table...")

        # Get the image names for the specified method
        measure_df = etl.get_measure_image_names(conn)

        if measure_df.empty:
            raise click.UsageError(
                "The ingest-main-method-images command can only be used after "
                "ingesting the main methdology floor measures."
            )

        for image_type, image_path in [("panorama", pano_path), ("lidar", lidar_path)]:
            if image_path:
                click.echo(f"Ingesting {image_type} images...")

                filename_field = (
                    "clip_path" if image_type == "panorama" else "lidar_clip_path"
                )

                image_df = measure_df[["id", filename_field]].copy()
                image_df = image_df.rename(columns={"id": "floor_measure_id"})

                # Get image filepaths
                if image_type == "panorama":
                    image_df[filename_field] = image_df[filename_field].apply(
                        lambda filename: Path(image_path) / Path(filename).name
                    )
                else:
                    # TODO Set lidar_clip_path to full path
                    image_df[filename_field] = image_df[filename_field].apply(
                        lambda filename: list(
                            (Path(image_path) / Path(filename).name).glob("*")
                        )[0]
                    )

                # Add additional fields
                image_df["filename"] = image_df[filename_field].apply(
                    lambda path: Path(path).name
                )
                image_df["type"] = image_type

                # Create byte arrays of the images
                image_df["image_data"] = image_df[filename_field].apply(
                    lambda filename: image_to_bytearray(filename)
                )

                image_df = image_df[image_df["image_data"].notna()]

                image_df = image_df.drop(
                    columns=[
                        filename_field,
                    ],
                    axis=1,
                )

                image_df.to_sql(
                    "floor_measure_image",
                    conn,
                    schema="public",
                    if_exists="append",
                    index=False,
                    dtype={
                        "image_data": LargeBinary,
                        "floor_measure_id": UUID,
                        "type": String,
                    },
                )

    click.echo("Image ingestion complete")


@click.command()
@click.option("-o", "--output-file", required=True, type=str, help="Path of output OGR dataset.")  # fmt: skip
@click.option("--normalise-aux-info", is_flag=True, help="Normalise the aux_info field into separate columns.")  # fmt: skip
@click.option("--buildings-only", is_flag=True, help="Export buildings only, for input to object detection processing.")  # fmt: skip
def export_ogr_file(output_file: str, normalise_aux_info: bool, buildings_only: bool):
    """Export an OGR file of the data model

    Supports GeoPackage, Shapefile and GeoJSON by specifying file extension in the
    output file name.
    """
    if normalise_aux_info and buildings_only:
        raise click.UsageError("Can't use --normalise-aux-info with --buildings-only")

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


@click.command()
@click.option("-i", "--input-json", required=True, type=click.File(), help="Path to FFH model output JSON.")  # fmt: skip
@click.option("--s3-uri", required=True, type=str, help="Root S3 URI path containing the images.")  # fmt: skip
@click.option("--areas", type=click.Choice(["Wagga", "Tweed", "Launceston"], case_sensitive=False), default=["Wagga", "Tweed", "Launceston"], multiple=True, help="The areas to download associated images, option can be used multiple times to download multiple areas.")  # fmt: skip
@click.option("--type", type=click.Choice(["pano", "lidar"], case_sensitive=False), default=["pano", "lidar"], multiple=True, help="The types of images to download, option can be used multiple times to download multiple types.")  # fmt: skip
@click.option("-o", "--output-dir", required=True, type=click.Path(file_okay=False, dir_okay=True, writable=True), help="Local directory to save downloaded images, images will be download into sub directories for each image type.")  # fmt: skip
def download_images_s3(
    input_json: click.File,
    s3_uri: str,
    areas: list,
    type: list,
    output_dir: click.Path,
):
    """Download main methodology images from S3

    Downloads images from an S3 bucket based on the provided JSON file and saves them to
    a local directory.

    Requires the S3 URI to be in the format 's3://bucket-name/prefix/', and is the path
    to the root of each region containing the images.

    AWS credentials must be configured in the environment or via the AWS CLI.
    """
    click.secho("Downloading images from S3", bold=True)
    try:
        json_data = json.load(input_json)
        json_df = pd.DataFrame(json_data["buildings"])
    except Exception as error:
        raise click.exceptions.FileError(input_json.name, error)

    json_df = json_df[~json_df["floor_height_consensus"].isna()]
    json_df = json_df.drop_duplicates(subset=["building_id", "floor_height_consensus"])

    json_df = json_df[json_df.region.isin(areas)]

    s3 = boto3.client("s3")
    bucket_name, prefix = s3_uri.strip("/").replace("s3://", "").split("/", 1)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for image_type in type:
        folder = "clips" if image_type == "pano" else "3d_point_clouds"
        type_output_dir = output_dir / (f"{image_type}_images")
        type_output_dir.mkdir(parents=True, exist_ok=True)

        # Create image prefixes for panorama images
        if image_type == "pano":
            json_df["image_prefixes"] = (
                json_df.region.astype(str)
                + "/"
                + folder
                + "/"
                + json_df.building_id.astype(str)
                + "_"
                + json_df.gnaf_id.astype(str)
                + "/"
                + json_df.best_view_pano_filename.astype(str).apply(
                    lambda x: Path(x).stem
                )
            )
        # Create image prefixes for lidar images
        elif image_type == "lidar":
            json_df["image_prefixes"] = (
                json_df.region.astype(str)
                + "/"
                + folder
                + "/"
                + json_df.building_id.astype(str)
                + "_"
                + json_df.gnaf_id.astype(str)
                + "/"
                + json_df.building_id.astype(str)
                + "_"
                + json_df.gnaf_id.astype(str)
                + "_3d_point_cloud.jpg"
            )

        with click.progressbar(
            json_df["image_prefixes"], label=f"Downloading {image_type} images"
        ) as bar:
            for image_prefix in bar:
                s3_key_prefix = f"{prefix}/{image_prefix}"
                try:
                    response = s3.list_objects_v2(
                        Bucket=bucket_name, Prefix=s3_key_prefix
                    )
                    if "Contents" in response:
                        for obj in response["Contents"]:
                            s3_key = obj["Key"]
                            local_file_path = type_output_dir / Path(s3_key).name
                            s3.download_file(bucket_name, s3_key, str(local_file_path))
                    else:
                        click.echo(
                            f"Warning: No images found for prefix {s3_key_prefix}",
                            err=True,
                        )
                except NoCredentialsError:
                    raise click.UsageError("AWS credentials not found.")
                except PartialCredentialsError:
                    raise click.UsageError("Incomplete AWS credentials configuration.")
                except Exception as error:
                    click.echo(f"Failed to download {s3_key_prefix}: {error}", err=True)

    click.echo("Image download complete")


cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)
cli.add_command(join_address_buildings)
cli.add_command(ingest_nexis_measures)
cli.add_command(ingest_validation_measures)
cli.add_command(ingest_main_method_measures)
cli.add_command(ingest_gap_fill_measures)
cli.add_command(ingest_main_method_images)
cli.add_command(export_ogr_file)
cli.add_command(download_images_s3)


if __name__ == "__main__":
    cli(max_content_width=250)


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli(max_content_width=250)
