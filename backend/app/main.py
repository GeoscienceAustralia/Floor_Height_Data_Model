import json
import logging
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from logging.config import dictConfig

import geoalchemy2
import geoalchemy2.functions
import sqlalchemy
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from geojson_pydantic import FeatureCollection
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.log import LogConfig
dictConfig(LogConfig().dict())
logger = logging.getLogger("floorheights")

from floorheights.datamodel.models import (
    AddressPoint,
    Building,
    Dataset,
    FloorMeasure,
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
    version='0.0.1',
    dependencies=[Depends(security)],
    lifespan=lifespan
)


def authenticated(credentials: HTTPBasicCredentials = Depends(security)):
    username = os.environ['APP_USERNAME']
    password = os.environ['APP_PASSWORD']

    if (
        (username is None or len(username) == 0) and 
        (password is None or len(password) == 0)
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


@app.get(
    "/api/address-point-to-building/{address_point_id}/geom/",
    response_model=FeatureCollection
)
def read_source_ids(
    address_point_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication = Depends(authenticated)
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
                    geoalchemy2.functions.ST_Centroid(Building.outline)
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


# TODO: this should really just be based on the SQLAlchemy model
# instead of redefining it
class FloorMeasureWeb(BaseModel):
    id: str
    storey: int
    height: float
    accuracy_measure: float
    aux_info: dict | None = None
    method: str
    datasets: list[str]


@app.get(
    "/api/floor-height-data/{building_id}",
    response_model=list[FloorMeasureWeb],
)
def get_floor_height_data(
    building_id: str,
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication = Depends(authenticated)
):
    if Authentication:
        pass
    uuid_id = uuid.UUID(building_id)

    result: Building = db.get(Building, uuid_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Building not found")

    floor_measures: list[FloorMeasureWeb] = []
    fm_db: FloorMeasure
    for fm_db in result.floor_measures:
        datasets = [ds.name for ds in fm_db.datasets]
        floor_measure = FloorMeasureWeb(
            id=str(fm_db.id),
            storey=fm_db.storey,
            height=fm_db.height,
            accuracy_measure=fm_db.accuracy_measure,
            aux_info=fm_db.aux_info,
            method=fm_db.method.name,
            datasets=datasets
        )
        floor_measures.append(floor_measure)

    return floor_measures


@app.get(
    "/api/methods/",
    response_model=list[str],
)
def list_methods(
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication = Depends(authenticated)
):
    # return a simple list of all methods sorted alphabetically
    if Authentication:
        pass
    return [
        r[0] 
        for r in db.query(Method.name).order_by(Method.name)
    ]


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


@app.get("/api/geojson/", response_class=StreamingResponse)
def export_geojson(
    db: sqlalchemy.orm.Session = Depends(get_db),
    Authentication=Depends(authenticated)
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
