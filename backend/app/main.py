from fastapi import FastAPI, Depends
import geoalchemy2.functions
import geoalchemy2.functions
from geojson_pydantic import FeatureCollection
import sqlalchemy
import geoalchemy2
from sqlalchemy import select
import uuid
import json

from floorheights.datamodel.models import (
    AddressPoint, Building,
    SessionLocal
)


description = """
The Floor Heights API provides access to the Floor Heights
Data model through a series of rest endpoints.

## Acknowledgements
Developed by FrontierSI (https://frontiersi.com.au/)
"""

app = FastAPI(
    title="Floor Heights API",
    description=description,
    version='0.0.1',
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/api/")
def read_root():
    return {"Hello": "Floor Heights API"}


@app.get(
    "/api/address-point-to-building/{address_point_id}/geom/",
    response_model=FeatureCollection
)
def read_source_ids(address_point_id: str, db: sqlalchemy.orm.Session = Depends(get_db)):
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
