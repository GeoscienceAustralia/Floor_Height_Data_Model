import click
import csv
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from collections.abc import Iterable
from io import StringIO
from rasterio.mask import mask
from shapely.geometry import box
from sqlalchemy import Table, Numeric, select, and_, not_, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql import Select, func, exists
from sqlalchemy.sql.expression import TableClause
from uuid import UUID

from floorheights.datamodel.models import AddressPoint, Building, FloorMeasure, Method, Dataset, address_point_building_association, floor_measure_dataset_association, SessionLocal


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
def join_address_buildings(input_cadastre):
    """Join address points to building outlines"""
    session = SessionLocal()
    engine = session.get_bind()
    Base = declarative_base()

    click.echo("Performing join by contains...")
    # Selects address-building matches by buldings containing address points
    select_query = select(
        AddressPoint.id.label("address_point_id"), Building.id.label("building_id")
    ).join(Building, func.ST_Contains(Building.outline, AddressPoint.location))

    # TODO: May want to consider using a materialised view instead of these queries

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
            click.echo(f"An error occurred while loading the file: {error}")
            session.rollback()
            return

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
                and_(
                    func.ST_Area(
                        func.ST_Intersection(Building.outline, temp_cadastre.c.geometry)
                    )
                    / func.ST_Area(Building.outline)
                    > 0.5,
                    ~exists().where(
                        address_point_building_association.c.address_point_id
                        == AddressPoint.id
                    ),
                )
            )
        )

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


def build_floor_measure_query(
    temp_nexis: TableClause,
    method_id: UUID,
    accuracy_measure: float,
    storey: int,
    step_counting: bool = False,
) -> Select:
    """Build a SQL select query to insert into FloorMeasure with conditional filters"""
    query = (
        select(
            func.gen_random_uuid().label("id"),
            literal(storey).label("storey"),
            temp_nexis.c.floor_height_m.label("height"),
            literal(accuracy_measure).label("accuracy_measure"),
            Building.id.label("building_id"),
            literal(method_id).label("method_id"),
            func.json_build_object(
                'nexis_construction_type', temp_nexis.c.nexis_construction_type,
                'nexis_year_built', temp_nexis.c.nexis_year_built,
                'nexis_wall_type', temp_nexis.c.nexis_wall_type,
                'generic_ext_wall', temp_nexis.c.generic_ext_wall,
                'local_year_built', temp_nexis.c.local_year_built
            ).label("aux_info"),
        )
        .join(
            address_point_building_association,
            address_point_building_association.c.building_id == Building.id,
        )
        .join(
            AddressPoint,
            address_point_building_association.c.address_point_id == AddressPoint.id,
        )
        .join(
            temp_nexis,
            temp_nexis.c.lid == AddressPoint.gnaf_id,
        )
        .filter(not_(Building.id.in_(select(FloorMeasure.building_id))))
    )

    if step_counting:
        query = query.filter(func.mod(temp_nexis.c.floor_height_m, 0.28) == 0)

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
        .on_conflict_do_nothing()
        .returning(FloorMeasure.id)
    )
    return ids.all()


def insert_floor_measure_dataset_association(
    session: Session, nexis_dataset_id: UUID, floor_measure_inserted_ids: list
) -> None:
    """Insert records into the floor_measure_dataset_association table from a
    NEXIS Dataset record id and a list of FloorMeasure ids"""
    # Parse list of ids into a dict for inserting into the association table
    floor_measure_dataset_values = [
        {"floor_measure_id": row.id, "dataset_id": nexis_dataset_id}
        for row in floor_measure_inserted_ids
    ]
    session.execute(
        insert(floor_measure_dataset_association)
        .values(floor_measure_dataset_values)
        .on_conflict_do_nothing()
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
        temp_nexis, step_count_id, 50, 0, step_counting=True
    )
    survey_query = build_floor_measure_query(
        temp_nexis, survey_id, 90, 0, step_counting=False
    )

    click.echo("Inserting records into floor_measure table...")
    # Insert into the floor_measure table and the ids of records inserted
    step_count_ids = insert_floor_measure(session, step_count_query)
    survey_ids = insert_floor_measure(session, survey_query)

    floor_measure_inserted_ids = step_count_ids + survey_ids

    if floor_measure_inserted_ids:
        # If there are new floor_measure ids, get a dataset record for NEXIS
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


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)
cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)
cli.add_command(join_address_buildings)
cli.add_command(ingest_nexis_method)


if __name__ == '__main__':
    cli()


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli()
