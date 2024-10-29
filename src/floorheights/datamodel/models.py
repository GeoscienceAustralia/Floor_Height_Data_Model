from geoalchemy2 import Geometry
from sqlalchemy import (
    create_engine, Column, String, UUID, Float, Integer, JSON, Table, ForeignKey
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import uuid

from floorheights.datamodel.db_utils import create_database_url


Base = declarative_base()


address_point_building_association = Table(
    'address_point_building_association',
    Base.metadata,
    Column('address_point_id', UUID(as_uuid=True), ForeignKey('address_point.id'), primary_key=True),
    Column('building_id', UUID(as_uuid=True), ForeignKey('building.id'), primary_key=True)
)

floor_measure_dataset_association = Table(
    'floor_measure_dataset_association',
    Base.metadata,
    Column('floor_measure_id', UUID(as_uuid=True), ForeignKey('floor_measure.id'), primary_key=True),
    Column('dataset_id', UUID(as_uuid=True), ForeignKey('dataset.id'), primary_key=True)
)



class AddressPoint(Base):
    __tablename__ = 'address_point'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default="gen_random_uuid()")
    gnaf_id = Column(String(15), nullable=True)
    address = Column(String, nullable=False)
    location = Column(Geometry(geometry_type='POINT', srid=4326), nullable=False)

    # Many-to-Many relationship with Building
    buildings = relationship(
        'Building',
        secondary=address_point_building_association,
        back_populates='address_points'
    )

    def __repr__(self) -> str:
        return f"AddressPoint(id={self.id!r}, address={self.address!r}, location={self.location!r})"


class Building(Base):
    __tablename__ = 'building'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, server_default="gen_random_uuid()")
    outline = Column(Geometry(geometry_type='POLYGON', srid=4326), nullable=False)
    height_ahd = Column(Float, nullable=False)

    # Many-to-Many relationship with AddressPoint
    address_points = relationship(
        'AddressPoint',
        secondary=address_point_building_association,
        back_populates='buildings'
    )

    # One-to-many relationship to FloorMeasure
    floor_measures = relationship('FloorMeasure', back_populates='building')


class FloorMeasure(Base):
    __tablename__ = 'floor_measure'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    storey = Column(Integer, nullable=False)
    height = Column(Float, nullable=False)
    accuracy_measure = Column(Float, nullable=False)
    aux_info = Column(JSON, nullable=True)

    # Foreign key to Building
    building_id = Column(UUID(as_uuid=True), ForeignKey('building.id'), nullable=False)
    # Many-to-one relationship to Building
    building = relationship('Building', back_populates='floor_measures')

    # Foreign key to Method
    method_id = Column(UUID(as_uuid=True), ForeignKey('method.id'), nullable=False)
    # Many-to-one relationship to Method
    method = relationship('Method')

    # Many-to-many relationship to Dataset
    datasets = relationship(
        'Dataset',
        secondary=floor_measure_dataset_association,
        back_populates='floor_measures'
    )


class Method(Base):
    __tablename__ = 'method'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)


class Dataset(Base):
    __tablename__ = 'dataset'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    source = Column(String, nullable=True)

    # Many-to-many relationship to FloorMeasure
    floor_measures = relationship(
        'FloorMeasure',
        secondary=floor_measure_dataset_association,
        back_populates='datasets'
    )


# Database connection setup
engine = create_engine(create_database_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
