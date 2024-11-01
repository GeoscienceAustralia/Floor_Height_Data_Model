import click
import geopandas as gpd
import rasterio
from shapely.geometry import box
from sqlalchemy.orm import Session
from floorheights.datamodel.models import AddressPoint, Building, SessionLocal


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
        height_ahd=20.0
    )
    session.add(building)
    session.commit()

    click.echo("Dummy building added")


@click.command()
@click.option("-i", "--infile", "infile", required=True, type=click.Path(), help="Input address points (Geodatabase) file path.")
@click.option("-c", "--chunksize", "chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")
def ingest_address_points(infile, chunksize):
    """Ingest address points"""
    session = SessionLocal()
    engine = session.get_bind()

    click.echo("Loading Geodatabase...")
    address = gpd.read_file(infile, columns=["ADDRESS_DETAIL_PID", "COMPLETE_ADDRESS"])
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


@click.command()
@click.option("-i", "--infile", "infile", required=True, type=click.File(), help="Input building footprint (GeoParquet) file path.")
@click.option("-d", "--dem", "dem", required=True, type=click.File(), help="Input DEM file path.")
@click.option("-c", "--chunksize", "chunksize", type=int, default=None, help="Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.")
def ingest_buildings(infile, dem, chunksize):
    """Ingest building footprints"""
    session = SessionLocal()
    engine = session.get_bind()

    click.echo("Loading DEM...")
    dem = rasterio.open(dem.name)
    dem_crs = dem.crs

    click.echo("Creating mask...")
    bounds = dem.bounds
    mask_geom = box(*bounds)

    mask_df = gpd.GeoDataFrame({"id":1,"geometry":[mask_geom]}, crs=dem_crs.to_string())
    mask_df = mask_df.to_crs(4326)  # Transform mask to WGS84 - might be slightly offset buildings are in GDA94/GDA2020
    mask_bbox = mask_df.total_bounds

    click.echo("Loading building GeoParquet...")
    buildings = gpd.read_parquet(infile.name, columns=["geometry"], bbox=mask_bbox)
    buildings = buildings[buildings.geom_type == "Polygon"]  # Remove multipolygons
    
    click.echo("Sampling DEM with buildings...")
    buildings = buildings.to_crs(dem_crs.to_epsg())  # Transform buildings to CRS of our DEM
    centroids = [(x, y) for x, y in zip(buildings["geometry"].centroid.x, buildings["geometry"].centroid.y)]
    buildings["height_ahd"] = [x[0] for x in dem.sample(centroids)]
    buildings["height_ahd"] = buildings["height_ahd"].astype(float).round(3)

    buildings = buildings[buildings["height_ahd"] != dem.nodata]  # Remove any buildings that sample no data
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


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)
cli.add_command(ingest_address_points)
cli.add_command(ingest_buildings)


if __name__ == '__main__':
    cli()


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli()
