import click
import csv
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from collections.abc import Iterable
from io import StringIO
from pathlib import Path
from rasterio.mask import mask
from shapely.geometry import box
from sqlalchemy import Table, Numeric, select, and_, not_, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql import Select, func, exists, text
from sqlalchemy.sql.expression import TableClause, BinaryExpression
from typing import Literal
from uuid import UUID

from floorheights.datamodel.models import (
    AddressPoint,
    Building,
    FloorMeasure,
    Method,
    Dataset,
    address_point_building_association,
    floor_measure_dataset_association,
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
        gnaf_id='GANSW717206574',
        address='2 BENTLEY PLACE, WAGGA WAGGA, NSW 2650',
        location='SRID=4326;POINT(147.377214 -35.114780)'
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
            'SRID=4326;'
            'POLYGON ((147.37761655448156 -35.11448724509989, 147.37778526244756 -35.11466902926723,'
            '147.37788066971024 -35.11463666981137, 147.37775733837083 -35.11443775405107, '
            '147.37761655448156 -35.11448724509989))'
        ),
        min_height_ahd=179.907,
        max_height_ahd=180.155
    )
    session.add(building)
    session.commit()

    click.echo("Dummy building added")


@click.command()
@click.option("-i", "--input-address", "input_address", required=True, type=click.Path(), help="Input address points (Geodatabase) file path.")
@click.option("-c", "--chunksize", "chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")
def ingest_address_points(input_address, chunksize):
    """Ingest address points"""
    session = SessionLocal()
    engine = session.get_bind()

    click.echo("Loading Geodatabase...")
    address = gpd.read_file(input_address, columns=["ADDRESS_DETAIL_PID", "COMPLETE_ADDRESS"])
    address = address.to_crs(4326)

    address = address.rename(
        columns={"COMPLETE_ADDRESS": "address", "ADDRESS_DETAIL_PID": "gnaf_id"}
    )
    address = address.rename_geometry("location")

    click.echo("Copying to PostgreSQL...")
    address.to_postgis(
        "address_point",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        chunksize=chunksize,
    )

    click.echo("Address ingestion complete")


def sample_dem_with_buildings(dem: rasterio.io.DatasetReader, buildings: gpd.GeoDataFrame) -> tuple:
    """Sample minimum and maximum elevation values from a DEM for each building geometry"""
    min_heights = []
    max_heights = []

    for geom in buildings.geometry:
        # Mask the raster with the buildings, setting out of bounds pixels to NaN
        try:
            out_img, out_transform = mask(dem, [geom], crop=True, all_touched=True, nodata=np.nan)
            # Calculate min and max heights, ignoring NaN values
            min_height = np.nanmin(out_img)
            max_height = np.nanmax(out_img)
        except ValueError:  # In case building is out of raster bounds or empty
            min_height = dem.nodata
            max_height = dem.nodata
        min_heights.append(min_height)
        max_heights.append(max_height)

    return min_heights, max_heights


@click.command()
@click.option("-i", "--input-buildings", "input_buildings", required=True, type=click.File(), help="Input building footprint (GeoParquet) file path.")
@click.option("-d", "--input-dem", "dem_file", required=True, type=click.File(), help="Input DEM file path.")
@click.option("-c", "--chunksize", "chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")
def ingest_buildings(input_buildings, dem_file, chunksize):
    """Ingest building footprints"""
    session = SessionLocal()
    engine = session.get_bind()

    click.echo("Loading DEM...")
    dem = rasterio.open(dem_file.name)
    dem_crs = dem.crs

    click.echo("Creating mask...")
    bounds = dem.bounds
    mask_geom = box(*bounds)
    mask_df = gpd.GeoDataFrame({"id": 1, "geometry": [mask_geom]}, crs=dem_crs.to_string())
    mask_df = mask_df.to_crs(4326)  # Transform mask to WGS84 - might be slightly offset buildings are in GDA94/GDA2020
    mask_bbox = mask_df.total_bounds

    click.echo("Loading building GeoParquet...")
    buildings = gpd.read_parquet(input_buildings.name, columns=["geometry"], bbox=mask_bbox)
    buildings = buildings[buildings.geom_type == "Polygon"]  # Remove multipolygons

    click.echo("Sampling DEM with buildings...")
    buildings = buildings.to_crs(dem_crs.to_epsg())  # Transform buildings to CRS of our DEM
    min_heights, max_heights = sample_dem_with_buildings(dem, buildings)
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
    buildings.to_postgis(
        "building",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        chunksize=chunksize,
    )

    click.echo("Building ingestion complete")


@click.command()
@click.option("-c", "--input-cadastre", "input_cadastre", required=False, type=str, help="Input cadastre vector file path to support address joining.")
@click.option("--join-largest-building", "join_largest", is_flag=True, help="Join addresses to the largest building on the lot. This can help reduce the number of false matches to non-dwellings.")
def join_address_buildings(input_cadastre, join_largest):
    """Join address points to building outlines"""
    session = SessionLocal()
    engine = session.get_bind()
    Base = declarative_base()

    if join_largest and not input_cadastre:
        raise click.UsageError("--join-largest-building must be used with --input-cadastre")

    click.echo("Performing join by contains...")
    # Selects address-building matches by buldings containing address points
    select_query = select(
        AddressPoint.id.label("address_point_id"), Building.id.label("building_id")
    ).join(Building, func.ST_Contains(Building.outline, AddressPoint.location))

    insert_query = (
        insert(address_point_building_association)
        .from_select(["address_point_id", "building_id"], select_query)
        .on_conflict_do_nothing()
    )
    session.execute(insert_query)

    if input_cadastre:
        click.echo("Loading cadastre...")
        try:
            cadastre_df = gpd.read_file(input_cadastre, columns=["geometry"])
            cadastre_df = cadastre_df.to_crs(4326)
        except Exception as error:
            session.rollback()
            raise click.FileError(Path(input_cadastre).name, error)

        click.echo("Copying cadastre to PostgreSQL...")
        cadastre_df.to_postgis(
            "temp_cadastre",
            engine,
            schema="public",
            if_exists="replace",
            index=True,
            index_label="id",
        )

        # Get temp_cadastre table model from database
        temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=engine)

        # TODO: May need an extra processing step to deal with overlapping parcel geometries

        click.echo("Performing join with cadastre...")
        # Selects address-building matches by joining to common cadastre lots, where buildings overlap the lot by 50% of its area
        select_query = (
            select(
                AddressPoint.id.label("address_point_id"),
                Building.id.label("building_id"),
            )
            .select_from(
                AddressPoint.__table__.join(
                    temp_cadastre,
                    func.ST_Within(AddressPoint.location, temp_cadastre.c.geometry),
                ).join(
                    Building,
                    func.ST_Intersects(Building.outline, temp_cadastre.c.geometry),
                )
            )
            .where(

        if join_largest:
            click.echo("Joining with largest building on lot...")
            # Join to largest building for distinct address
            select_query = select_query.order_by(
                AddressPoint.id,
                func.ST_Area(Building.outline).desc()
            ).distinct(AddressPoint.id)

        insert_query = (
            insert(address_point_building_association)
            .from_select(["address_point_id", "building_id"], select_query)
            .on_conflict_do_nothing()
        )
        session.execute(insert_query)
        session.commit()

        temp_cadastre.drop(engine)

    session.commit()
    click.echo("Joining complete")


def psql_insert_copy(
    table: pd.io.sql.SQLTable,
    conn: Engine | Connection,
    keys: list[str],
    data_iter: Iterable,
) -> None:
    """Execute SQL statement inserting data

    Source: https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def get_or_create_method_id(session: Session, method_name: str) -> UUID:
    """Retrieve the ID for a given method, creating it if it doesn't exist"""
    method_id = session.execute(select(Method.id).filter(Method.name == method_name)).first()
    if not method_id:
        click.echo(f"Inserting '{method_name}' into method table...")
        method = Method(name=method_name)
        session.add(method)
        session.flush()
        return method.id
    return method_id[0]


def get_or_create_dataset_id(
    session: Session, dataset_name: str, dataset_desc: str, dataset_src: str
) -> UUID:
    """Retrieve the ID for a given dataset, creating it if it doesn't exist"""
    dataset_id = session.execute(
        select(Dataset.id).filter(Dataset.name == dataset_name)
    ).first()
    if not dataset_id:
        click.echo(f"Inserting '{dataset_name}' into dataset table...")
        dataset = Dataset(
            name=dataset_name,
            description=dataset_desc,
            source=dataset_src
        )
        session.add(dataset)
        session.flush()
        return dataset.id
    return dataset_id[0]


def build_aux_info_expression(table: Table, ignore_columns: list) -> BinaryExpression:
    """Dynamically build json_build_object expression for the aux_info field

    Input data (particularly validation data) will come from multiple sources,
    so the number of arguments to the jsonb_build_object function will differ and
    could exceed 100 (i.e. 50 fields).

    If so, the expression is separated into chunks of 100 arguments, which are
    then concatenated with the '||' operator.
    """
    columns = [col for col in table.columns if col.name not in ignore_columns]
    json_args = []
    json_args_chunked = []

    for column in columns:
        json_args.extend([text(f"'{column.name}'"), column])
        # When we reach 100 arguments (50 pairs), append the chunk and start a new one
        if len(json_args) >= 100:
            json_args_chunked.append(json_args)
            json_args = []

    # Append the chunk if there are remaining arguments
    if json_args:
        json_args_chunked.append(json_args)

    # Build the SQL expression with jsonb_build_object functions
    json_build_expr = func.jsonb_build_object(*json_args_chunked[0])

    for chunk in json_args_chunked[1:]:
        # Concatenate the chunks of json arguments using the '||' operator
        # so we don't exceed the 100 parameter limit
        json_build_expr = json_build_expr.op("||")(func.jsonb_build_object(*chunk))

    return json_build_expr


def build_floor_measure_query(
    floor_measure_table: TableClause,
    ffh_field: str,
    method_id: UUID,
    accuracy_measure: float,
    storey: int,
    join_by: Literal['gnaf_id', 'cadastre'],
    gnaf_id_col: str = None,
    step_counting: bool = None,
    step_size: float = None,
    cadastre: TableClause = None
) -> Select:
    """Build a SQL select query to insert into FloorMeasure with conditional filters"""
    query = select(
        func.gen_random_uuid().label("id"),
        literal(storey).label("storey"),
        floor_measure_table.c[ffh_field].label("height"),
        literal(accuracy_measure).label("accuracy_measure"),
        Building.id.label("building_id"),
        literal(method_id).label("method_id"),
        build_aux_info_expression(
            floor_measure_table, [ffh_field, "id", "geometry"]
        ).label("aux_info"),
    )

    if join_by == "gnaf_id":
        # Join by GNAF ID matching
        query = (
            query.select_from(Building)
            .join(AddressPoint, Building.address_points)
            .join(
                floor_measure_table,
                floor_measure_table.c[gnaf_id_col] == AddressPoint.gnaf_id,
            )
        )
    elif join_by == "cadastre":
        # Join floor_measure point to cadastre polygon by nearest neighbour
        # https://postgis.net/workshops/postgis-intro/knn.html
        lateral_subquery = (
            select(
                cadastre.c.id.label("id"),
                cadastre.c.geometry.label("geometry"),
                (floor_measure_table.c.geometry.op("<->")(cadastre.c.geometry)).label(
                    "dist"
                ),
            )
            .order_by("dist")
            .limit(1)
            .lateral()
        )
        query = (
            query.select_from(
                # Join on True to make it a cross join
                floor_measure_table.join(lateral_subquery, literal(True)).join(
                    AddressPoint,
                    func.ST_Within(AddressPoint.location, lateral_subquery.c.geometry),
                )
            )
            .join(Building, AddressPoint.buildings)
            .distinct(Building.id)
        )

    if step_counting is True and step_size:
        # Select floor heights divisible by step_size
        query = query.filter(func.mod(floor_measure_table.c[ffh_field], step_size) == 0)
    elif step_counting is False and step_size:
        # Select floor_heights not divisible by step_size
        # This retrieves the remaining floor heights for inserting into a different method
        query = query.filter(
            not_(func.mod(floor_measure_table.c[ffh_field], step_size) == 0)
        )
    return query


def insert_floor_measure(session: Session, select_query: Select) -> list:
    """Insert records into the FloorMeasure table from a select query,
    returning a list of the floor_measure ids that were inserted
    """
    ids = session.execute(
        insert(FloorMeasure)
        .from_select(
            ["id", "storey", "height", "accuracy_measure", "building_id", "method_id", "aux_info"],
            select_query,
        )
        .returning(FloorMeasure.id)
    )
    return ids.all()


def insert_floor_measure_dataset_association(
    session: Session, nexis_dataset_id: UUID, floor_measure_inserted_ids: list
) -> None:
    """Insert records into the floor_measure_dataset_association table from a
    NEXIS Dataset record id and a list of FloorMeasure ids
    """
    # Parse list of ids into a dict for inserting into the association table
    floor_measure_dataset_values = [
        {"floor_measure_id": row.id, "dataset_id": nexis_dataset_id}
        for row in floor_measure_inserted_ids
    ]
    session.execute(
        insert(floor_measure_dataset_association)
        .values(floor_measure_dataset_values)
    )


@click.command()
@click.option("-i", "--input-nexis", "input_nexis", required=True, type=click.File(), help="Input NEXIS CSV file path.")
def ingest_nexis_method(input_nexis):
    """Ingest NEXIS floor height method"""

    session = SessionLocal()
    engine = session.get_bind()
    Base = declarative_base()

    click.echo("Loading NEXIS points...")
    nexis_df = pd.read_csv(
        input_nexis,
        usecols=[
            "LID",
            "floor_height_(m)",
            "flood_vulnerability_function_id",
            "NEXIS_CONSTRUCTION_TYPE",
            "NEXIS_YEAR_BUILT",
            "NEXIS_WALL_TYPE",
            "GENERIC_EXT_WALL",
            "LOCAL_YEAR_BUILT",
        ],
        dtype={
            "LID": str,
            "floor_height_(m)": float,
            "flood_vulnerability_function_id": str,
            "NEXIS_CONSTRUCTION_TYPE": str,
            "NEXIS_YEAR_BUILT": str,  # Some records include year ranges
            "NEXIS_WALL_TYPE": str,
            "GENERIC_EXT_WALL": str,
            "LOCAL_YEAR_BUILT": str,  # Some records include year ranges
        },
    )
    # Make column names lower case and remove parenthesis
    nexis_df.columns = nexis_df.columns.str.lower().str.replace(
        r"\(|\)", "", regex=True
    )
    nexis_df = nexis_df[
        nexis_df["lid"].str.startswith("GNAF")
    ]  # Drop rows that aren't a GNAF address
    nexis_df["lid"] = nexis_df["lid"].str[5:]  # Remove "GNAF_" prefix

    # Select gnaf_ids in the database
    gnaf_ids = session.execute(select(AddressPoint.gnaf_id)).all()
    gnaf_ids = [row[0] for row in gnaf_ids]

    # Subset NEXIS data based on selection
    nexis_df = nexis_df[nexis_df["lid"].isin(gnaf_ids)]

    click.echo("Copying NEXIS points to PostgreSQL...")
    nexis_df.to_sql(
        "temp_nexis",
        engine,
        method=psql_insert_copy,
        if_exists="replace",
        dtype={
            "floor_height_m": Numeric  # Set numeric so we don't need to type cast in the db
        },
    )
    temp_nexis = Table("temp_nexis", Base.metadata, autoload_with=engine)

    # Get step_count and survey method ids
    step_count_id = get_or_create_method_id(session, "Step counting")
    survey_id = get_or_create_method_id(session, "Surveyed")

    # TODO: Determine measure accuracies

    # Build select query that will be inserted into the floor_measure table
    step_count_query = build_floor_measure_query(
        temp_nexis,
        "floor_height_m",
        step_count_id,
        50,
        0,
        join_by="gnaf_id",
        gnaf_id_col="lid",
        step_counting=True,
        step_size=0.28,
    )
    survey_query = build_floor_measure_query(
        temp_nexis,
        "floor_height_m",
        survey_id,
        90,
        0,
        join_by="gnaf_id",
        gnaf_id_col="lid",
        step_counting=False,
        step_size=0.28,
    )

    click.echo("Inserting records into floor_measure table...")
    # Insert into the floor_measure table and get the ids of records inserted
    step_count_ids = insert_floor_measure(session, step_count_query)
    survey_ids = insert_floor_measure(session, survey_query)
    floor_measure_inserted_ids = step_count_ids + survey_ids

    if floor_measure_inserted_ids:
        nexis_dataset_id = get_or_create_dataset_id(
            session, "NEXIS", "NEXIS building points", "Geoscience Australia"
        )
        insert_floor_measure_dataset_association(
            session, nexis_dataset_id, floor_measure_inserted_ids
        )

    session.commit()

    temp_nexis.drop(engine)
    session.commit()

    click.echo("NEXIS ingestion complete")


@click.command()
@click.option("-i", "--input-data", "input_data", required=True, type=str, help="Input validation OGR dataset file path.")
@click.option("-c", "--input-cadastre", "input_cadastre", required=True, type=str, help="Input cadastre OGR dataset file path to support address joining.")
@click.option("--ffh-field", "ffh_field", type=str, required=True, help="Name of the first floor height field.")
@click.option("--step-size", "step_size", type=float, required=False, default=0.28, show_default=True, help="Step size value in metres.")
@click.option("--dataset-name", "dataset_name", type=str, required=False, help="Dataset name.")
@click.option("--dataset-desc", "dataset_desc", type=str, required=False, help="Dataset description.")
@click.option("--dataset-src", "dataset_src", type=str, required=False, help="Dataset source.")
def ingest_validation_method(
    input_data,
    input_cadastre,
    ffh_field,
    step_size,
    dataset_name,
    dataset_desc,
    dataset_src,
):
    """Ingest validation floor height method"""
    session = SessionLocal()
    engine = session.get_bind()
    Base = declarative_base()

    # Read datasets into GeoDataFrames
    try:
        method_df = gpd.read_file(input_data)
        method_df = method_df.to_crs(4326)
    except Exception as error:
        raise click.exceptions.FileError(Path(input_data).name, error)
    try:
        cadastre_df = gpd.read_file(input_cadastre, columns=["geometry"])
        cadastre_df = cadastre_df.to_crs(4326)
    except Exception as error:
        raise click.exceptions.FileError(Path(input_data).name, error)

    if ffh_field not in method_df.columns:
        raise click.exceptions.BadParameter(f"Field '{ffh_field}' not found in input file")

    method_df = method_df.rename(columns={ffh_field: "floor_height_m"})
    # Make method input column names lower case and remove special characters
    method_df.columns = method_df.columns.str.lower().str.replace(
        r"\W+", "", regex=True
    )

    click.echo("Copying validation table to PostgreSQL...")
    method_df.to_postgis(
        "temp_method",
        engine,
        schema="public",
        if_exists="replace",
        index=True,
        index_label="id",
        dtype={"floor_height_m": Numeric},  # Set numeric so we don't need to type cast in the db
    )
    click.echo("Copying cadastre to PostgreSQL...")
    cadastre_df.to_postgis(
        "temp_cadastre",
        engine,
        schema="public",
        if_exists="replace",
        index=True,
        index_label="id",
    )

    # Get temp table models from database
    temp_method = Table("temp_method", Base.metadata, autoload_with=engine)
    temp_cadastre = Table("temp_cadastre", Base.metadata, autoload_with=engine)

    step_count_id = get_or_create_method_id(session, "Step counting")
    survey_id = get_or_create_method_id(session, "Surveyed")

    # Build select queries that will be inserted into the floor_measure table
    step_count_query = build_floor_measure_query(
        temp_method,
        "floor_height_m",
        step_count_id,
        50,
        0,
        join_by="cadastre",
        step_counting=True,
        step_size=step_size,
        cadastre=temp_cadastre,
    )
    survey_query = build_floor_measure_query(
        temp_method,
        "floor_height_m",
        survey_id,
        90,
        0,
        join_by="cadastre",
        step_counting=False,
        step_size=step_size,
        cadastre=temp_cadastre,
    )

    click.echo("Inserting records into floor_measure table...")
    # Insert into the floor_measure table and get the ids of records inserted
    step_count_ids = insert_floor_measure(session, step_count_query)
    survey_ids = insert_floor_measure(session, survey_query)
    validation_inserted_ids = step_count_ids + survey_ids

    if validation_inserted_ids:
        if not dataset_name:
            dataset_name = Path(input_data).name

        nexis_dataset_id = get_or_create_dataset_id(
            session, dataset_name, dataset_desc, dataset_src
        )
        insert_floor_measure_dataset_association(
            session, nexis_dataset_id, validation_inserted_ids
        )

    session.commit()

    temp_method.drop(engine)
    temp_cadastre.drop(engine)
    session.commit()

    click.echo("Validation ingestion complete")


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)
cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)
cli.add_command(join_address_buildings)
cli.add_command(ingest_nexis_method)
cli.add_command(ingest_validation_method)


if __name__ == '__main__':
    cli()


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli()
