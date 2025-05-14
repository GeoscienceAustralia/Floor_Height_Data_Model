import csv
import geopandas as gpd
import json
import numpy as np
import pandas as pd
import rasterio
import uuid
from collections.abc import Iterable
from geoalchemy2 import Geometry, Geography
from io import StringIO
from pyproj import CRS
from rasterio.mask import mask
from sqlalchemy import (
    Table,
    Column,
    Result,
    Integer,
    Select,
    BinaryExpression,
    select,
    delete,
    not_,
    exists,
    text,
    literal,
    func,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session, InstrumentedAttribute, DeclarativeMeta, aliased
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


def read_ogr_file(input_file: str, **kwargs) -> gpd.GeoDataFrame:
    """Read OGR file into GeoDataFrame

    If the input OGR file's geodetic datum is GDA1994, transform it to GDA2020.
    Subsequently transform to WGS1984 for ingestion into PostgreSQL.
    """
    gdf = gpd.read_file(input_file, **kwargs)
    if gdf.crs.geodetic_crs.equals(CRS.from_epsg(4283).geodetic_crs):
        gdf = gdf.to_crs(7844)
    gdf = gdf.to_crs(4326)
    return gdf


def read_nexis_csv(input_nexis: str, crs: int = 4283) -> gpd.GeoDataFrame:
    """Read NEXIS CSV file into GeoDataFrame

    If the CSV's geodetic datum is GDA1994 (which is assumed by default), transform it
    to GDA2020. Subsequently transform to WGS1984 for ingestion into PostgreSQL.
    """
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
            "LATITUDE",
            "LONGITUDE",
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
            "LATITUDE": float,
            "LONGITUDE": float,
        },
    )

    # Make NEXIS input column names lower case and remove special characters
    nexis_df.columns = nexis_df.columns.str.lower().str.replace(r"\W+", "", regex=True)
    # Remove "_GNAF" prefix
    nexis_df.lid = nexis_df.lid.str.removeprefix("GNAF_")

    nexis_gdf = gpd.GeoDataFrame(
        nexis_df,
        geometry=gpd.GeoSeries.from_xy(nexis_df.longitude, nexis_df.latitude, crs=crs),
    )
    nexis_gdf = nexis_gdf.drop(columns=["latitude", "longitude"])
    if nexis_gdf.crs.geodetic_crs.equals(CRS.from_epsg(4283).geodetic_crs):
        nexis_gdf = nexis_gdf.to_crs(7844)
    nexis_gdf = nexis_gdf.to_crs(4326)

    return nexis_gdf


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

        columns = ", ".join([f'"{k}"' for k in keys])
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


def sample_dem_with_buildings(
    dem: rasterio.io.DatasetReader, buildings: gpd.GeoDataFrame
) -> tuple:
    """Sample minimum and maximum elevation values from a DEM for each building geometry"""
    min_heights = []
    max_heights = []

    for geom in buildings.geometry:
        # Mask the raster with the buildings, setting out of bounds pixels to NaN
        try:
            out_img, out_transform = mask(
                dem, [geom], crop=True, all_touched=True, nodata=np.nan
            )
            # Calculate min and max heights, ignoring NaN values
            min_height = np.nanmin(out_img)
            max_height = np.nanmax(out_img)
        except ValueError:  # In case building is out of raster bounds or empty
            min_height = dem.nodata
            max_height = dem.nodata
        min_heights.append(min_height)
        max_heights.append(max_height)

    return min_heights, max_heights


def remove_overlapping_geoms(
    session: Session, overlap_threshold: float, bbox: tuple = None
) -> Result:
    """Remove overlapping geometries within a bounding box"""
    # Alias so we can perform self-comparison
    smaller = aliased(Building, name="smaller")
    larger = aliased(Building, name="larger")

    # Define a lateral subquery that finds larger buildings intersecting a smaller building
    lateral_subquery = (
        select(
            larger.id,
            larger.outline,
        )
        .where(
            # Exclude identical buildings
            smaller.id != larger.id,
            # Ensure the 'smaller' building is actually smaller
            func.ST_Area(smaller.outline) < func.ST_Area(larger.outline),
            # Only consider buildings that intersect
            func.ST_Intersects(smaller.outline, larger.outline),
        )
        .lateral()  # Use lateral to evaluate this subquery for each smaller building individually
    )

    # Main query to select distinct larger buildings that significantly overlap with smaller ones
    select_query = (
        select(
            lateral_subquery.c.id,
        ).select_from(smaller)
        # Join on True to make it a cross lateral join
        .join(lateral_subquery, literal(True))
        # Calculate the ratio of the intersection
        .where(
            (
                func.ST_Area(
                    func.ST_Intersection(smaller.outline, lateral_subquery.c.outline)
                )
                / func.ST_Area(smaller.outline)
            )
            # If the ratio exceeds a threshold we select it for deletion
            > overlap_threshold
        )
    )

    # If a bounding box is provided, filter buildings within the bbox
    if bbox is not None:
        bbox_geom = func.ST_MakeEnvelope(*bbox, 4326)
        select_query = select_query.where(func.ST_Intersects(smaller.outline, bbox_geom))

    select_query = select_query.distinct(lateral_subquery.c.id)

    delete_stmt = delete(Building).where(Building.id.in_(select_query))
    return session.execute(delete_stmt)


def split_by_cadastre(
    address_points: gpd.GeoDataFrame,
    buildings: gpd.GeoDataFrame,
    cadastre: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Split geometries by cadastre boundaries"""
    # Spatial join to find points within buildings
    address_in_buildings = gpd.sjoin(
        address_points, buildings, how="inner", predicate="within"
    )

    # Count unique points for each building
    unique_address_count = address_in_buildings.groupby("index_right")[
        "location"
    ].apply(lambda x: len(set(x)))

    # Filter buildings with multiple unique points
    buildings_to_split = buildings.loc[
        unique_address_count[unique_address_count > 1].index
    ]

    # Split these buildings by cadastre lots
    split_buildings = gpd.overlay(buildings_to_split, cadastre, how="intersection")

    # Update the original GeoDataFrame
    buildings.loc[buildings_to_split.index, "geometry"] = None
    buildings = gpd.GeoDataFrame(
        pd.concat([buildings.geometry, split_buildings.geometry], ignore_index=True),
        crs=buildings.crs,
    )
    buildings = buildings.explode()
    return buildings


def flatten_cadastre_geoms(
    session: Session, conn: Connection, Base, temp_cadastre: Table
) -> Table:
    """flatten cadastre geometries"""
    boundaries_subquery = (
        select(func.ST_Dump(temp_cadastre.c.geometry).geom.label("geometry"))
        .select_from(temp_cadastre)
        .subquery()
    )

    boundaries_cte = select(
        func.ST_Union(func.ST_ExteriorRing(boundaries_subquery.c.geometry)).label(
            "geometry"
        )
    ).cte()

    select_query = select(
        func.ST_Dump(func.ST_Polygonize(boundaries_cte.c.geometry)).geom.label(
            "geometry"
        ),
    )

    # Create a temporary table to insert the select query into
    flat_temp_cadastre = Table(
        "flat_temp_cadastre",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("geometry", Geometry(geometry_type="POLYGON", srid=4326)),
    )
    flat_temp_cadastre.create(conn)

    insert_query = insert(flat_temp_cadastre).from_select(["geometry"], select_query)
    session.execute(insert_query)

    temp_cadastre.drop(conn)  # Drop the original temp_cadastre table
    # Rename the flat_temp_cadastre table
    session.execute(text("ALTER TABLE flat_temp_cadastre RENAME TO temp_cadastre"))

    # Return flat cadastre metadata for subsequent joining
    return Table("temp_cadastre", Base.metadata, autoload_with=conn)


def join_by_contains(
    select_fields: list[InstrumentedAttribute | Column],
    point_geom: InstrumentedAttribute | Column,
) -> Select:
    """Join by contains helper function"""
    select_query = select(*select_fields).join(
        Building, func.ST_Contains(Building.outline, point_geom)
    )

    return select_query


def join_by_cadastre(
    select_fields: list[InstrumentedAttribute | Column],
    point_table: DeclarativeMeta | Table,
    point_geom: InstrumentedAttribute | Column,
    cadastre_table: Table,
    cadastre_geom: Column,
) -> Select:
    """Join by cadastre helper function"""
    select_query = (
        select(*select_fields)
        .select_from(point_table)
        .join(
            cadastre_table,
            func.ST_Within(point_geom, cadastre_geom),
        )
        .join(
            Building,
            func.ST_Intersects(Building.outline, cadastre_geom),
        )
        .where(
            # Join points where a building overlaps the lot by 50% of its area
            func.ST_Area(func.ST_Intersection(Building.outline, cadastre_geom))
            / func.ST_Area(Building.outline)
            > 0.5,
        )
    )

    return select_query


def join_by_knn(
    lateral_fields: list[InstrumentedAttribute | Column],
    point_table: DeclarativeMeta | Table,
    point_geom: InstrumentedAttribute | Column,
    additional_select_fields: list[InstrumentedAttribute | Column],
    cadastre_table: Table = None,
    cadastre_geom: Column = None,
    knn_max_distance: int = 10,
) -> Select:
    """Join by K-Nearest Neighbour helper function

    Source: https://postgis.net/workshops/postgis-intro/knn.html
    """
    lateral_subquery = (
        select(
            *lateral_fields,
            Building.outline.label("outline"),
            (point_geom.op("<->")(Building.outline)).label("dist"),
        )
        .order_by("dist")
        .limit(1)
        .lateral()
    )

    select_query = select(
        *additional_select_fields,
        lateral_subquery.c.building_id.label("building_id"),
    ).select_from(point_table)

    if cadastre_table is None:
        # Nearest neighbour join for all addresses, if we don't have a cadastre
        select_query = select_query.join(lateral_subquery, literal(True)).where(
            func.ST_Distance(
                func.cast(point_geom, Geography),
                func.cast(lateral_subquery.c.outline, Geography),
            )
            < knn_max_distance,
        )
    else:
        # Nearest neighbour join for addresses outside the cadastre extent
        select_query = (
            # Outer join so we only take points outside the cadastre
            select_query.outerjoin(
                cadastre_table,
                func.ST_Within(point_geom, cadastre_geom),
            )
            .join(lateral_subquery, literal(True))
            .where(
                cadastre_geom == None,
                # Join addresses to nearest building if it is within a distance threshold
                func.ST_Distance(
                    func.cast(point_geom, Geography),
                    func.cast(lateral_subquery.c.outline, Geography),
                )
                < knn_max_distance,
            )
            .where(
                ~exists().where(
                    address_point_building_association.c.address_point_id
                    == AddressPoint.id,
                )
            )
        )

    return select_query


def build_address_match_query(
    join_by: Literal["intersects", "cadastre", "knn"],
    cadastre: Table = None,
    knn_max_distance: int = 10,
) -> Select:
    """Build address matching query"""
    select_fields = [
        AddressPoint.id.label("address_point_id"),
        Building.id.label("building_id"),
    ]

    if join_by == "intersects":
        select_query = join_by_contains(select_fields, AddressPoint.location).where(
            AddressPoint.geocode_type == "BUILDING CENTROID"
        )
    elif join_by == "cadastre":
        select_query = join_by_cadastre(
            select_fields,
            AddressPoint,
            AddressPoint.location,
            cadastre,
            cadastre.c.geometry,
        ).where(
            AddressPoint.geocode_type == "PROPERTY CENTROID",
            # Don't join to any buildings already joined by within
            ~exists().where(
                address_point_building_association.c.building_id == Building.id,
            ),
        )
    elif join_by == "knn":
        if cadastre is not None:
            cadastre_geom = cadastre.c.geometry
        else:
            cadastre_geom = None

        lateral_fields = [
            AddressPoint.id.label("address_point_id"),
            Building.id.label("building_id"),
        ]
        additional_select_fields = [AddressPoint.id.label("address_point_id")]
        select_query = join_by_knn(
            lateral_fields,
            AddressPoint,
            AddressPoint.location,
            additional_select_fields=additional_select_fields,
            cadastre_table=cadastre,
            cadastre_geom=cadastre_geom,
            knn_max_distance=knn_max_distance,
        )

    return select_query


def insert_address_building_association(session: Session, select_query: Select):
    """Insert records into the address_point_building_association table from a select
    query"""
    insert_query = (
        insert(address_point_building_association)
        .from_select(["address_point_id", "building_id"], select_query)
        .on_conflict_do_nothing()
    )
    session.execute(insert_query)


def get_or_create_method_id(session: Session, method_name: str) -> uuid.UUID:
    """Retrieve the ID for a given method, creating it if it doesn't exist"""
    method_id = session.execute(
        select(Method.id).filter(Method.name == method_name)
    ).first()
    if not method_id:
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
        dataset = Dataset(
            name=dataset_name, description=dataset_desc, source=dataset_src
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
    floor_measure_table: Table,
    ffh_field: str,
    method_id: uuid.UUID,
    storey: int,
    accuracy_measure: float = None,
    join_by: Literal["gnaf_id", "intersects", "cadastre", "knn"] = None,
    gnaf_id_col: str = None,
    step_counting: bool = None,
    step_size: float = None,
    cadastre: Table = None,
) -> Select:
    """Build a SQL select query to insert into FloorMeasure with conditional filters"""
    if join_by:
        building_id_field = Building.id.label("building_id")
    else:
        building_id_field = floor_measure_table.c.building_id.label("building_id")
    if accuracy_measure is not None:
        accuracy_measure_field = literal(accuracy_measure)
    else:
        accuracy_measure_field = floor_measure_table.c.accuracy_measure

    select_fields = [
        floor_measure_table.c.id.label("id"),
        literal(storey).label("storey"),
        floor_measure_table.c[ffh_field].label("height"),
        accuracy_measure_field.label("accuracy_measure"),
        literal(method_id).label("method_id"),
        build_aux_info_expression(
            floor_measure_table, [ffh_field, "id", "geometry"]
        ).label("aux_info"),
        building_id_field
    ]

    if join_by == "gnaf_id":
        # Join by GNAF ID matching
        select_query = (
            select(*select_fields)
            .select_from(Building)
            .join(AddressPoint, Building.address_points)
            .join(
                floor_measure_table,
                floor_measure_table.c[gnaf_id_col] == AddressPoint.gnaf_id,
            )
        )
    elif join_by == "intersects":
        select_query = join_by_contains(select_fields, floor_measure_table.c.geometry)
    elif join_by == "cadastre":
        select_query = join_by_cadastre(
            select_fields,
            floor_measure_table,
            floor_measure_table.c.geometry,
            cadastre,
            cadastre.c.geometry,
        ).where(
            ~exists().where(
                FloorMeasure.building_id == building_id_field,
            )
        )
    elif join_by == "knn":
        if cadastre is not None:
            cadastre_geom = cadastre.c.geometry
        else:
            cadastre_geom = None

        # Remove building id from additional fields because we select it from the lateral subquery
        select_fields.remove(building_id_field)

        lateral_fields = [
            Building.id.label("building_id"),
        ]
        select_query = join_by_knn(
            lateral_fields,
            floor_measure_table,
            floor_measure_table.c.geometry,
            additional_select_fields=select_fields,
            cadastre_table=cadastre,
            cadastre_geom=cadastre_geom,
        )
    else:
        select_query = select(*select_fields).select_from(floor_measure_table)

    if step_counting is True and step_size:
        # Select floor heights divisible by step_size
        select_query = select_query.filter(
            func.mod(floor_measure_table.c[ffh_field], step_size) == 0
        )
    elif step_counting is False and step_size:
        # Select floor_heights not divisible by step_size
        # This retrieves the remaining floor heights for inserting into a different method
        select_query = select_query.filter(
            not_(func.mod(floor_measure_table.c[ffh_field], step_size) == 0)
        )
    return select_query


def insert_floor_measure(session: Session, select_query: Select) -> list:
    """Insert records into the FloorMeasure table from a select query,
    returning a list of the floor_measure ids that were inserted
    """
    ids = session.execute(
        insert(FloorMeasure)
        .from_select(
            [
                "id",
                "storey",
                "height",
                "accuracy_measure",
                "method_id",
                "aux_info",
                "building_id",
            ],
            select_query,
        )
        .on_conflict_do_nothing()
        .returning(FloorMeasure.id)
    )
    return list(ids.all())


def insert_floor_measure_dataset_association(
    session: Session, dataset_id: uuid.UUID, floor_measure_inserted_ids: list
) -> None:
    """Insert records into the floor_measure_dataset_association table from a Dataset
    record id and a list of FloorMeasure ids
    """
    # Parse list of ids into a dict for inserting into the association table
    floor_measure_dataset_values = [
        {"floor_measure_id": row, "dataset_id": dataset_id}
        for row in floor_measure_inserted_ids
    ]
    session.execute(
        insert(floor_measure_dataset_association).values(floor_measure_dataset_values)
    )


def build_denormalised_query() -> Select:
    """Build denormalised query"""
    select_query = (
        select(
            AddressPoint.gnaf_id,
            AddressPoint.address.label("gnaf_address"),
            Building.min_height_ahd.label("min_building_height_ahd"),
            Building.max_height_ahd.label("max_building_height_ahd"),
            Dataset.name.label("dataset"),
            Method.name.label("method"),
            FloorMeasure.storey,
            FloorMeasure.height.label("floor_height_m"),
            FloorMeasure.accuracy_measure.label("accuracy"),
            FloorMeasure.aux_info,
            Building.outline,
        )
        .select_from(FloorMeasure)
        .join(Method, FloorMeasure.method)
        .join(Dataset, FloorMeasure.datasets)
        .join(Building)
        .join(AddressPoint, Building.address_points)
    )

    return select_query


def write_ogr_file(
    output_file: str, select_query: Select, conn: Connection, normalise_aux_info=False
):
    gdf = gpd.read_postgis(select_query, con=conn, geom_col="outline")

    if normalise_aux_info is True:
        gdf = pd.concat([gdf, pd.json_normalize(gdf.pop("aux_info"))], axis=1)
    else:
        # Convert dict rows to JSON strings
        gdf.aux_info = gdf.aux_info.apply(json.dumps)

    gdf.to_file(output_file)
