from geoalchemy2 import Geometry
from sqlalchemy import create_engine, Column, String, UUID, Float, Integer, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from floorheights.datamodel.db_utils import create_database_url


Base = declarative_base()

class AddressPoint(Base):
    __tablename__ = 'address_point'
    id = Column(UUID(as_uuid=True), primary_key=True)
    address = Column(String, nullable=False)
    location = Column(Geometry(geometry_type='POINT'), nullable=False)


class Building(Base):
    __tablename__ = 'building'
    id = Column(UUID(as_uuid=True), primary_key=True)
    outline = Column(Geometry(geometry_type='POLYGON'), nullable=False)
    height_ahd = Column(Float, nullable=False)


class FloorMeasure(Base):
    __tablename__ = 'floor_measure'
    id = Column(UUID(as_uuid=True), primary_key=True)
    storey = Column(Integer, nullable=False)
    height = Column(Float, nullable=False)
    accuracy_measure = Column(Float, nullable=False)
    aux_info = Column(JSON, nullable=True)


class Method(Base):
    __tablename__ = 'method'
    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, nullable=False)


class Dataset(Base):
    __tablename__ = 'dataset'
    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    source = Column(String, nullable=True)


# Database connection setup
engine = create_engine(create_database_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
