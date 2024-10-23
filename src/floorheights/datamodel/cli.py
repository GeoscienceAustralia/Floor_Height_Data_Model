import click
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
        address='2 Bentley Pl, Wagga Wagga',
        location='SRID=4326;POINT(147.37777 -35.114555)'
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


cli.add_command(create_dummy_address_point)
cli.add_command(create_dummy_building)


if __name__ == '__main__':
    cli()


# as referenced in setup.py (is the CLI console_script function)
def main():
    cli()
