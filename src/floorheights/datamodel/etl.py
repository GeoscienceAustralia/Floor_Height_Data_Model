import click
import csv
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import uuid
from collections.abc import Iterable
from geoalchemy2 import Geometry, Geography
from io import StringIO
from pathlib import Path
from rasterio.mask import mask
from shapely.geometry import box
from sqlalchemy import Table, Column, Numeric, Integer, select, delete, and_, not_, text, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select, func
from sqlalchemy.sql.expression import TableClause, BinaryExpression
from typing import Literal

from floorheights.datamodel.models import (
    AddressPoint,
    Building,
    FloorMeasure,
    Method,
    Dataset,
    address_point_building_association,
    floor_measure_dataset_association,
)

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


def get_or_create_method_id(session: Session, method_name: str) -> uuid.UUID:
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
) -> uuid.UUID:
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
    method_id: uuid.UUID,
    accuracy_measure: float,
    storey: int,
    join_by: Literal["gnaf_id", "cadastre"],
    gnaf_id_col: str = None,
    step_counting: bool = None,
    step_size: float = None,
    cadastre: TableClause = None,
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
    session: Session, nexis_dataset_id: uuid.UUID, floor_measure_inserted_ids: list
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
