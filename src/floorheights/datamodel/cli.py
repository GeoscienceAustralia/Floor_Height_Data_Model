import click
import geopandas as gpd
import pandas as pd
import rasterio
import uuid
from pathlib import Path
from shapely.geometry import box
from sqlalchemy import Table, Numeric, UUID, select
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
@click.option("-i", "--input-address", required=True, type=str, help="Input address points (Geodatabase) file path.")  # fmt: skip
@click.option("-c", "--chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
def ingest_address_points(input_address, chunksize):
    """Ingest address points"""
    click.echo("Loading Geodatabase...")
    try:
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
@click.option("-i", "--input-buildings", required=True, type=click.File(), help="Input building footprint (GeoParquet) file path.")  # fmt: skip
@click.option("-d", "--input-dem", required=True, type=click.File(), help="Input DEM file path.")  # fmt: skip
@click.option("-c", "--chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")  # fmt: skip
@click.option("--remove-small", type=float, is_flag=False, flag_value=30, default=None, help="Remove smaller buildings, optionally specify an area threshold in square metres.  [default: 30.0]")  # fmt: skip
@click.option("--remove-overlapping", type=float, is_flag=True, flag_value=0.80, default=None, help="Remove overlapping buildings, optionally specify an intersection ratio threshold.  [default: 0.80]")  # fmt: skip
def ingest_buildings(
    input_buildings, input_dem, chunksize, remove_small, remove_overlapping
):
    """Ingest building footprints"""
    click.echo("Loading DEM...")
    try:
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

    click.echo("Loading building GeoParquet...")
    try:
        buildings = gpd.read_parquet(
            input_buildings.name, columns=["geometry"], bbox=mask_bbox
        )
    except Exception as error:
        raise click.exceptions.FileError(input_buildings.name, error)

    buildings = buildings[buildings.geom_type == "Polygon"]  # Remove multipolygons
    buildings = buildings.to_crs(
        dem_crs.to_epsg()
    )  # Transform buildings to CRS of our DEM

    if remove_small:
        click.echo(f"Removing buildings < {remove_small} m^2...")
        bool_mask = buildings.area > remove_small
        buildings = buildings[bool_mask]
        remove_count = (bool_mask).value_counts()[False]
        click.echo(f"Removed {remove_count} buildings...")

    click.echo("Sampling DEM with buildings...")
    min_heights, max_heights = etl.sample_dem_with_buildings(dem, buildings)
    buildings["min_height_ahd"] = min_heights
    buildings["max_height_ahd"] = max_heights
    buildings = buildings.round({"min_height_ahd": 3, "max_height_ahd": 3})

    # TODO: Handle building footprints overlapping the edge of the DEM

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
            result = etl.remove_overlapping_geoms(session, remove_overlapping)
            click.echo(f"Removed {result.rowcount} overlapping buildings...")

        click.echo("Building ingestion complete")


@click.command()
@click.option("-c", "--input-cadastre", type=str, help="Input cadastre vector file path to support address joining.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join addresses to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
def join_address_buildings(input_cadastre, flatten_cadastre, join_largest):
    """Join address points to building outlines"""
    if join_largest and not input_cadastre:
        raise click.UsageError(
            "--join-largest-building must be used with --input-cadastre"
        )
    if flatten_cadastre and not input_cadastre:
        raise click.UsageError("--flatten-cadastre must be used with --input-cadastre")

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()
        click.echo("Performing join by contains...")
        # Selects address-building matches for addresses geocoded to building centroids
        select_query = etl.build_address_match_query(cadastre=False, join_by="intersects")
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
            select_query = etl.build_address_match_query("knn", temp_cadastre)
            etl.insert_address_building_association(session, select_query)
        else:
            # If we don't use a cadastre, do a nearest-neighbour join
            select_query = etl.build_address_match_query("knn")
            etl.insert_address_building_association(session, select_query)

        if input_cadastre:
            temp_cadastre.drop(conn)

    click.echo("Joining complete")


@click.command()
@click.option("-i", "--input-nexis", required=True, type=click.File(), help="Input NEXIS CSV file path.")  # fmt: skip
@click.option("-c", "--input-cadastre", required=False, type=str, default=None, help="Input cadastre OGR dataset file path to support joining non-GNAF NEXIS points.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measures to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
def ingest_nexis_method(input_nexis, flatten_cadastre, join_largest, input_cadastre):
    """Ingest NEXIS floor height method"""
    click.echo("Loading NEXIS points...")
    try:
        nexis_gdf = etl.read_nexis_csv(input_nexis, 4283)
    except Exception as error:
        raise click.exceptions.FileError(input_nexis.name, error)

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()

        if input_cadastre:
            try:
                cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
            except Exception as error:
                raise click.exceptions.FileError(Path(input_cadastre).name, error)
            # Clip NEXIS points to cadastre extent
            nexis_gdf = gpd.clip(nexis_gdf, cadastre_gdf)
        else:
            # Subset NEXIS points based GNAF IDs in the database
            gnaf_ids = session.execute(select(AddressPoint.gnaf_id)).all()
            gnaf_ids = [row[0] for row in gnaf_ids]
            nexis_gdf = nexis_gdf[nexis_gdf["lid"].isin(gnaf_ids)]
        
        nexis_gdf['id'] = [uuid.uuid4() for _ in range(len(nexis_gdf.index))]
        nexis_gdf = nexis_gdf.set_index(['id'])

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
                temp_cadastre = etl.flatten_cadastre_geoms(session, conn, Base, temp_cadastre)

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
                # Modify select to join to largest building on the lot for distinct address
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

            modelled_inserted_ids += (
                modelled_query_intersect_ids
                + modelled_inserted_cadastre_ids
                + modelled_inserted_cadastre_knn_ids
            )

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
@click.option("-i", "--input-data", required=True, type=str, help="Input validation OGR dataset file path.")  # fmt: skip
@click.option("-c", "--input-cadastre", required=True, type=str, help="Input cadastre OGR dataset file path to support address joining.")  # fmt: skip
@click.option("--flatten-cadastre", is_flag=True, help="Flatten cadastre by polygonising overlaps into one geometry per overlapped area. This can help reduce false matches.")  # fmt: skip
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join measures to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")  # fmt: skip
@click.option("--ffh-field", type=str, required=True, help="Name of the first floor height field.")  # fmt: skip
@click.option("--step-size", type=float, default=0.28, show_default=True, help="Step size value in metres.")  # fmt: skip
@click.option("--dataset-name", type=str, help="Dataset name.")
@click.option("--dataset-desc", type=str, help="Dataset description.")
@click.option("--dataset-src", type=str, help="Dataset source.")
def ingest_validation_method(
    input_data,
    input_cadastre,
    flatten_cadastre,
    join_largest,
    ffh_field,
    step_size,
    dataset_name,
    dataset_desc,
    dataset_src,
):
    """Ingest validation floor height method"""
    # Read datasets into GeoDataFrames
    try:
        method_gdf = etl.read_ogr_file(input_data)
    except Exception as error:
        raise click.exceptions.FileError(Path(input_data).name, error)
    try:
        cadastre_gdf = etl.read_ogr_file(input_cadastre, columns=["geometry"])
    except Exception as error:
        raise click.exceptions.FileError(Path(input_cadastre).name, error)

    if ffh_field not in method_gdf.columns:
        raise click.exceptions.BadParameter(
            f"Field '{ffh_field}' not found in input file"
        )

    method_gdf = method_gdf.rename(columns={ffh_field: "floor_height_m"})
    # Make method input column names lower case and remove special characters
    method_gdf.columns = method_gdf.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )
    method_gdf['id'] = [uuid.uuid4() for _ in range(len(method_gdf.index))]
    method_gdf = method_gdf.set_index(['id'])

    session = SessionLocal()
    with session.begin():
        conn = session.connection()
        Base = declarative_base()
        click.echo("Copying validation table to PostgreSQL...")
        method_gdf.to_postgis(
            "temp_method",
            conn,
            schema="public",
            if_exists="replace",
            index=True,
            dtype={"id": UUID, "floor_height_m": Numeric},
        )
        click.echo("Copying cadastre to PostgreSQL...")
        cadastre_gdf.to_postgis(
            "temp_cadastre",
            conn,
            schema="public",
            if_exists="replace",
            index=True,
            index_label="id",
        )

        # Get temp table models from database
        temp_method = Table("temp_method", Base.metadata, autoload_with=conn)
        temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=conn)

        if flatten_cadastre:
            click.echo("Flattening cadastre geometries...")
            temp_cadastre = etl.flatten_cadastre_geoms(session, conn, Base, temp_cadastre)

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
            cadastre=temp_cadastre,
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
            cadastre=temp_cadastre,
        )
        survey_intersects_ids = etl.insert_floor_measure(
            session, survey_query_intersects
        )

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
            # Modify select to join to largest building on the lot for distinct address
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

        # Insert into the floor_measure table and get the ids of records inserted
        step_count_knn_ids = etl.insert_floor_measure(
            session, step_count_query_knn
        )
        survey_knn_ids = etl.insert_floor_measure(
            session, step_count_query_knn
        )

        validation_inserted_ids = (
            step_count_intersects_ids
            + survey_intersects_ids
            + step_count_cadastre_knn_ids
            + survey_cadastre_knn_ids
            + step_count_knn_ids
            + survey_knn_ids
        )

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
        temp_cadastre.drop(conn)

        click.echo("Validation ingestion complete")


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)
cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)
cli.add_command(join_address_buildings)
cli.add_command(ingest_nexis_method)
cli.add_command(ingest_validation_method)


if __name__ == "__main__":
    cli()


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli()
