import json
import logging
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from io import BytesIO
from logging.config import dictConfig
from urllib.parse import urljoin

import geoalchemy2
import geoalchemy2.functions
import httpx
import sqlalchemy
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from geojson_pydantic import Feature, FeatureCollection
from PIL import Image, ImageDraw
from sqlalchemy import any_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func
from starlette.background import BackgroundTask
from starlette.requests import Request

from app.log import LogConfig

dictConfig(LogConfig().dict())
logger = logging.getLogger("floorheights")

from app.schemas import FloorMeasureResponse, GraduatedLegendResponse
from floorheights.datamodel.models import (
    AddressPoint,
    Building,
    Dataset,
    FloorMeasure,
    FloorMeasureImage,
    Method,
    SessionLocal,
)

from app.martin import setup_building_layer_fn

description = """
The Floor Heights API provides access to the Floor Heights
Data model through a series of rest endpoints.

## Acknowledgements
Developed by FrontierSI (https://frontiersi.com.au/)
"""


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Do app initialization stuff here before the yield
    db = next(get_db())
    setup_building_layer_fn(db)
    yield
    # Clean up anything on server shutdown here
    pass


security = HTTPBasic()
app = FastAPI(
    title="Floor Heights API",
    description=description,
    version="0.0.1",
    dependencies=[Depends(security)],
    lifespan=lifespan,
)


def authenticated(credentials: HTTPBasicCredentials = Depends(security)):
    username = os.environ["APP_USERNAME"]
    password = os.environ["APP_PASSWORD"]

    if (username is None or len(username) == 0) and (
        password is None or len(password) == 0
    ):
        # no auth if username and password have not been set
        return True

    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = username.encode("utf8")
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = password.encode("utf8")
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/api/")
def read_root():
    return {"Hello": "Floor Heights API"}


@app.get("/api/building/{building_id}/geom/", response_model=Feature)
def get_building_geom(
    building_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass

    uuid_id = uuid.UUID(building_id)

    result: Building = db.get(Building, uuid_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Building not found")

    try:
        data = db.execute(
            select(geoalchemy2.functions.ST_AsGeoJSON(Building.outline)).where(
                Building.id == uuid_id
            )
        ).one_or_none()
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail="Database error")

    if not data or not data[0]:
        raise HTTPException(status_code=404, detail="Building geometry not found")

    geojson_feature = {
        "type": "Feature",
        "geometry": json.loads(data[0]),
        "properties": {},
    }

    return Feature(**geojson_feature)


@app.get(
    "/api/address-point-to-building/{address_point_id}/geom/",
    response_model=FeatureCollection,
)
def read_source_ids(
    address_point_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass
    uuid_id = uuid.UUID(address_point_id)

    # do the join between addresses and buildings, craft a select statement that builds
    # line geom between address point and the building centroid
    data = db.execute(
        select(
            geoalchemy2.functions.ST_AsGeoJSON(
                geoalchemy2.functions.ST_MakeLine(
                    AddressPoint.location,
                    geoalchemy2.functions.ST_Centroid(Building.outline),
                )
            ),
        )
        .where(AddressPoint.id == uuid_id)
        .join(Building, AddressPoint.buildings)
    ).all()

    # convert DB results to a pydantic geojson model we can return
    features: list[dict] = []
    for row in data:
        geojson_feature = {
            "type": "Feature",
            "geometry": json.loads(row[0]),
            "properties": {},
        }
        features.append(geojson_feature)

    fc = FeatureCollection(type="FeatureCollection", features=features)
    return fc


@app.get(
    "/api/floor-height-data/{building_id}",
    response_model=list[FloorMeasureResponse],
)
def get_floor_height_data(
    building_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass
    uuid_id = uuid.UUID(building_id)

    result: Building = db.get(Building, uuid_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Building not found")

    floor_measures: list[FloorMeasureResponse] = []
    fm_db: FloorMeasure
    for fm_db in result.floor_measures:
        datasets = [ds.name for ds in fm_db.datasets]
        floor_measure = FloorMeasureResponse(
            id=str(fm_db.id),
            storey=fm_db.storey,
            height=fm_db.height,
            accuracy_measure=fm_db.accuracy_measure,
            aux_info=fm_db.aux_info,
            method=fm_db.method.name,
            datasets=datasets,
        )
        floor_measures.append(floor_measure)

    return floor_measures


@app.get(
    "/api/methods/",
    response_model=list[str],
)
def list_methods(
    db: sqlalchemy.orm.Session = Depends(get_db), Authentication=Depends(authenticated)
):
    # return a simple list of all methods sorted alphabetically
    if Authentication:
        pass
    return [r[0] for r in db.query(Method.name).order_by(Method.name)]


@app.get(
    "/api/legend-graduated-values/",
    response_model=GraduatedLegendResponse,
)
def get_legend_graduated_values(
    method_filter: str | None = "",
    dataset_filter: str | None = "",
    bbox: str | None = "",
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass

    # Parse query parameters into sorted lists
    if method_filter:
        method_filter_list = [x.strip() for x in method_filter.split(",")]
        method_filter_list.sort()
    if dataset_filter:
        dataset_filter_list = [x.strip() for x in dataset_filter.split(",")]
        dataset_filter_list.sort()
    if bbox:
        bbox_list = [x.strip() for x in bbox.split(",")]

    subquery = (
        db.query(
            func.avg(FloorMeasure.height).label("avg_ffh"),
        )
        .select_from(Building)
        .join(FloorMeasure)
        .join(Method, FloorMeasure.method)
        .join(Dataset, FloorMeasure.datasets)
        .filter(
            (method_filter == "" or Method.name.like(any_(method_filter_list))),
            (dataset_filter == "" or Dataset.name.in_(dataset_filter_list)),
            (
                bbox == ""
                or func.ST_Contains(
                    func.ST_MakeEnvelope(*bbox_list, 7844), Building.outline
                )
            ),
        )
        .group_by(Building.id)
    ).subquery()

    query = db.query(
        func.min(subquery.c.avg_ffh),
        func.max(subquery.c.avg_ffh),
    )

    return GraduatedLegendResponse(min=query[0][0], max=query[0][1])


@app.get(
    "/api/legend-categorised-values/{table}",
    response_model=list[str],
)
def get_legend_categorised_values(
    table=str,
    method_filter: str | None = "",
    dataset_filter: str | None = "",
    bbox: str | None = "",
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass

    if table == "dataset":
        field = Dataset.name
    elif table == "method":
        field = Method.name
    else:
        raise HTTPException(status_code=400, detail=f"Invalid table parameter: {table}")

    # Parse query parameters into sorted lists
    if method_filter:
        method_filter_list = [x.strip() for x in method_filter.split(",")]
        method_filter_list.sort()
    if dataset_filter:
        dataset_filter_list = [x.strip() for x in dataset_filter.split(",")]
        dataset_filter_list.sort()
    if bbox:
        bbox_list = [x.strip() for x in bbox.split(",")]

    query = (
        db.query(func.string_agg(func.distinct(field), ", ").label("values"))
        .select_from(Building)
        .join(FloorMeasure)
        .join(Method, FloorMeasure.method)
        .join(Dataset, FloorMeasure.datasets)
        .distinct()
        .filter(
            (method_filter == "" or Method.name.like(any_(method_filter_list))),
            (dataset_filter == "" or Dataset.name.in_(dataset_filter_list)),
            (
                bbox == ""
                or func.ST_Contains(
                    func.ST_MakeEnvelope(*bbox_list, 7844), Building.outline
                )
            ),
        )
        .group_by(Building.id)
    )

    # Sort results
    result_list = [result.values for result in query.all()]
    sorted_result_list = sorted(
        result_list,
        key=lambda x: (
            len(x.split(", ")) != 1,  # Single items first
            x.lower(),  # Then alphabetically
        ),
    )

    return sorted_result_list


@app.get(
    "/api/datasets/",
    response_model=list[str],
)
def list_datasets(
    db: sqlalchemy.orm.Session = Depends(get_db), Authentication=Depends(authenticated)
):
    if Authentication:
        pass
    return [r[0] for r in db.query(Dataset.name).order_by(Dataset.name)]


def query_geojson(db: sqlalchemy.orm.Session = Depends(get_db)):
    try:
        query = (
            db.query(
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
                geoalchemy2.functions.ST_AsGeoJSON(Building.outline).label("geometry"),
            )
            .select_from(FloorMeasure)
            .join(Method, FloorMeasure.method)
            .join(Dataset, FloorMeasure.datasets)
            .join(Building)
            .join(AddressPoint, Building.address_points)
        ).yield_per(1000)

        data_returned = False
        for feature in query:
            data_returned = True
            yield {
                "type": "Feature",
                "geometry": json.loads(feature.geometry),
                "properties": {
                    "gnaf_id": feature.gnaf_id,
                    "address": feature.gnaf_address,
                    "min_building_height_ahd": feature.min_building_height_ahd,
                    "max_building_height_ahd": feature.max_building_height_ahd,
                    "dataset": feature.dataset,
                    "method": feature.method,
                    "storey": feature.storey,
                    "floor_height_m": feature.floor_height_m,
                    "accuracy": feature.accuracy,
                    "aux_info": feature.aux_info,
                },
            }
        if not data_returned:
            logger.warning("No data returned from the database query.")
            raise ValueError("No data found for the given query.")

    except SQLAlchemyError as e:
        logger.error(f"Database query failed: {e}")
        raise RuntimeError("An error occurred while querying the database.") from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse geometry as JSON: {e}")
        raise ValueError("Invalid geometry data encountered.") from e


@app.get(
    "/api/image-ids/{building_id}",
    response_model=list[uuid.UUID],
)
def get_pano_image_ids(
    building_id: str,
    type: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass
    uuid_id = uuid.UUID(building_id)

    if type not in ["panorama", "lidar"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid type parameter. Must be 'panorama' or 'lidar'.",
        )

    query = (
        select(FloorMeasureImage.id)
        .select_from(FloorMeasureImage)
        .join(FloorMeasure)
        .join(Building)
        .filter(Building.id == uuid_id)
        .filter(FloorMeasureImage.type == type)
    )

    results = db.execute(query).all()

    return [r[0] for r in results]


def __color_for_class(class_name: str) -> str:
    """
    Returns a color for the given class name
    """
    class_colors = {
        "Window": "blue",
        "Front Door": "green",
        "Garage Door": "orange",
        "Foundation": "pink",
    }
    # default to red if class not found
    return class_colors.get(class_name, "red")


def draw_object_detection_boxes(
    image_data: bytes, aux_info: dict | None = None
) -> bytes:
    """
    Draws bounding boxes on the image based on the aux_info.
    """
    if aux_info is None:
        return image_data

    try:
        image = Image.open(BytesIO(image_data))
        draw = ImageDraw.Draw(image)

        for obj in aux_info.get("object_detections", []):
            box = obj.get("bbox_xyxy")
            class_name = obj.get("class")
            class_color = __color_for_class(class_name)
            confidence = obj.get("confidence")
            confidence = f"{confidence:.2f}"
            # draw detection bbox
            if box:
                x1, y1, x2, y2 = box
                draw.rectangle([x1, y1, x2, y2], outline=class_color, width=4)

            # following three vars should be enough to tweak the font size, and the boxes
            # drawn behind for readbility
            line_padding = 4
            padding = 6
            font_size = 20
            class_name_length_px = draw.textlength(class_name, font_size=font_size)
            confidence_length_px = draw.textlength(confidence, font_size=font_size)
            # draw some filled boxes that will be behind the text for readability
            draw.rectangle(
                [
                    x1,
                    y1,
                    x1 + 2 * padding + class_name_length_px,
                    y1 + 2 * padding + font_size,
                ],
                fill=class_color,
            )
            draw.rectangle(
                [
                    x1,
                    y1 + 2 * padding + font_size,
                    x1 + 2 * padding + confidence_length_px,
                    y1 + 3 * padding + 2 * font_size,
                ],
                fill=class_color,
            )
            draw.text(
                (x1 + padding, y1 + padding),
                f"{class_name}",
                fill="white",
                font_size=font_size,
            )
            draw.text(
                (x1 + padding, y1 + padding + line_padding + font_size),
                confidence,
                fill="white",
                font_size=font_size,
            )
        output_buffer = BytesIO()
        image.save(output_buffer, format="JPEG")
        return output_buffer.getvalue()
    except Exception as e:
        # if any part of the drawing fails, log the error and return the original image
        logger.error(f"Error drawing bounding boxes: {e}")
        return image_data


@app.get(
    "/api/image/{image_id}",
    response_class=StreamingResponse,
)
def get_image(
    image_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated),
):
    if Authentication:
        pass
    uuid_id = uuid.UUID(image_id)

    query = (
        select(
            FloorMeasureImage.image_data, FloorMeasureImage.type, FloorMeasure.aux_info
        )
        .select_from(FloorMeasureImage)
        .join(FloorMeasure)
        .filter(FloorMeasureImage.id == uuid_id)
    )
    result = db.execute(query).fetchone()

    if result is None:
        raise HTTPException(status_code=404, detail="Image not found.")

    image_data = result[0]

    if result[1] == "panorama":
        image_data = draw_object_detection_boxes(image_data, result[2])

    return StreamingResponse(
        BytesIO(image_data),
        media_type="image/jpeg",
    )


@app.get("/api/geojson/", response_class=StreamingResponse)
def export_geojson(
    db: sqlalchemy.orm.Session = Depends(get_db), Authentication=Depends(authenticated)
):
    if Authentication:
        pass

    def generate():
        try:
            yield '{"type": "FeatureCollection", "features": [\n'
            first = True
            for feature in query_geojson(db):
                if not first:
                    yield ",\n"
                yield json.dumps(feature)
                first = False
            yield "\n]}"
        except ValueError as e:
            logger.error(f"Query returned no data: {e}")
            raise HTTPException(status_code=404, detail="No data found for the query.")
        except RuntimeError as e:
            logger.error(f"Error generating GeoJSON: {e}")
            raise HTTPException(status_code=500, detail="Failed to generate GeoJSON.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred.")

    return StreamingResponse(generate(), media_type="application/json")


MAPS_HOST = os.getenv("MAPS_HOST")
if not MAPS_HOST:
    raise ValueError("Environment variable 'MAPS_HOST' is not set.")
MAPS_SERVER = httpx.AsyncClient(base_url=MAPS_HOST)


@app.get("/api/maps/{path:path}", response_class=StreamingResponse)
async def map_proxy(
    request: Request,
    path: str,
    Authentication=Depends(authenticated),
):
    """
    Proxy for the map service
    """
    # Authentication is enforced via Depends(authenticated)
    # Construct the target URL
    # a valid url should be something like:
    # http://martin:3000/building_query/13/7450/4949
    # Construct the target URL with query parameters
    query_string = request.url.query
    target_url = urljoin(MAPS_HOST, f"{path}")
    if query_string:
        target_url = f"{target_url}?{query_string}"

    rp_req = MAPS_SERVER.build_request(
        request.method,
        target_url,
        headers=request.headers.raw,
        content=await request.body(),
    )
    rp_resp = await MAPS_SERVER.send(rp_req, stream=True)
    return StreamingResponse(
        rp_resp.aiter_raw(),
        status_code=rp_resp.status_code,
        headers=rp_resp.headers,
        background=BackgroundTask(rp_resp.aclose),
    )
