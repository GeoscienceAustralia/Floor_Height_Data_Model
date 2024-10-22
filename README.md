# GA Floor Height Data Model
Data model for the GA floor height project. Includes;
- Model definition defined using ORM (SQLAlchemy)
- Migration scripts and tools for generating migrations for data model schema changes
- Command line tool to support working with the data model
- Docker files for launching suitable environment


## Getting started

Build the docker images

    docker compose build

Startup the database (Postgres) container

    docker compose up

Apply all database migrations, this will build the data model tables

    docker compose run app alembic upgrade head


## Changing data model schema
Alembic is used to automatically generate database migrations scripts from changes
to the SQLAlchemy model definitions. The steps required to modify the schema are as
follows.

First, make changes to the SQLAlchemy model definition by changing the models defined
in [`models.py`](./src/floorheights/datamodel/models.py).

Run the following command to automatically generate a new migration script.

    docker compose run app alembic revision --autogenerate -m "notes on migration changes"

This command will generate a new file in ['versions'](./src/alembic/versions/). This should be
checked over to ensure it is as expected. For some migrations, for example where data may need
to be migrated between tables, custom code will need to be included in the migration. **Although
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


