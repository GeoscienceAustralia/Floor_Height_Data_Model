from sqlalchemy import select
from sqlalchemy.orm import Session
from floorheights.datamodel.models import engine, AddressPoint

session = Session(engine)

sel_address_points = select(AddressPoint).limit(10)

for ap in session.scalars(sel_address_points):
    print(str(ap))

