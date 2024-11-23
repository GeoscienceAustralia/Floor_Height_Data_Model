from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from contextlib import asynccontextmanager
from typing import Annotated
import geoalchemy2.functions
import geoalchemy2.functions
from geojson_pydantic import FeatureCollection
from pydantic import BaseModel
import secrets
import sqlalchemy
import geoalchemy2
from sqlalchemy import select
from typing import Optional
import uuid
import json
import os
from logging.config import dictConfig
import logging

from app.log import LogConfig
dictConfig(LogConfig().dict())
logger = logging.getLogger("floorheights")

from floorheights.datamodel.models import (
    AddressPoint, Building, FloorMeasure, Method,
    SessionLocal
)

from app.martin import setup_building_layer_fn


description = """
The Floor Heights API provides access to the Floor Heights
Data model through a series of rest endpoints.

## Acknowledgements
Developed by FrontierSI (https://frontiersi.com.au/)
"""

security = HTTPBasic()
app = FastAPI(
    title="Floor Heights API",
    description=description,
    version='0.0.1',
    dependencies=[Depends(security)]
)


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
