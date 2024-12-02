# GA Floor Height Data Model

Data model for the GA floor height project. Includes;

- Model definition
  - Shown in figure below
  - Defined using ORM (SQLAlchemy)
- Migrations and tools for generating migrations based on data model changes
- Command line tool to support working with the data model
- Docker files to provide environment for CLI application and database

![Floor Heights data model schema diagram](./docs/floorheights_schema.png)

## Getting started

Create the `.env` file, this includes all the environment variables used by the application. The default values in this example file should be changed (password at least).

    cp .env.example .env

Build the docker images

    docker compose build

Startup the database (Postgres) container

    docker compose up

Apply all database migrations, this will create the data model tables in the database.

    docker compose run --rm app alembic upgrade head

Some dummy data can then be added using the following commands (for test purposes).

    docker compose run --rm app python -m floorheights.datamodel.cli create-dummy-address-point
    docker compose run --rm app python -m floorheights.datamodel.cli create-dummy-building

## Using as a library

The data model can be installed as used as a library. The following steps show one possible way to do this.

The database host must be running, and have floor heights data inserted.

Ensure the following environment variables are set. This can be done using environment variables, or a .env file

- POSTGRES_USER - user with access to database
- POSTGRES_PASSWORD - password for user
- POSTGRES_DB - name of the database
- POSTGRES_HOST - database hostname (e.g.; localhost if running postgres locally)

Create and activate a conda environment

    conda create -n floor_heights_test python=3.10
    conda activate floor_heights_test

Install an editable version of the floorheights-datamodel package. Assumes this repo has been cloned already. This command should install all necessary dependencies, if that process fails install dependencies from the requirements.txt using pip.

    pip install -e .

Create a `simple_select.py` file containing the following code. File can be found in [`./src/examples`](./src/examples/)

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from floorheights.datamodel.models import engine, AddressPoint

    session = Session(engine)

    sel_address_points = select(AddressPoint).limit(10)

    for ap in session.scalars(sel_address_points):
        print(str(ap))

Run the script. This will list up to 10 address points that have been added to the database.

    python simple_select.py

## Changing data model schema

Alembic is used to automatically generate database migrations scripts from changes
to the SQLAlchemy model definitions. The steps required to modify the schema are as
follows.

First, make changes to the SQLAlchemy model definition by changing the models defined
in [`models.py`](./src/floorheights/datamodel/models.py).

Run the following command to automatically generate a new migration script.

    docker compose run app alembic revision --autogenerate -m "notes on migration changes"

This command will generate a new file in [`versions`](./src/alembic/versions/). This should be
checked over to ensure it is as expected. Some migrations, for example where data may need
to be migrated between tables, will require custom code. **Although
programatically generated, all migration scripts should be included in git.**

To update the database with this new migration run the following. This will run all pending
migrations.

    docker compose run app alembic upgrade head

### Reverting schema changes

Say a bad migration was generated and applied to the database. The last migration can be
reverted by running

    docker compose run app alembic downgrade -1

The bad migration file should then be deleted from the ['versions'](./src/alembic/versions/)
folder.

[Alembic provides many options for dealing with these issues](https://alembic.sqlalchemy.org/).
