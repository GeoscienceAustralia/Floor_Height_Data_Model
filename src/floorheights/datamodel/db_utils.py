"""
Utilities for connecting to the database
"""

import os
from sqlalchemy import URL
from dotenv import load_dotenv, find_dotenv

# Load env vars from .env file
# This is done to support local (non-docker) development environments
load_dotenv(find_dotenv())

def create_database_url() -> URL:
    """General a DB url that will connect to the postgres container
    using env vars. Will raise a runtime error if one of the required
    environment variables is not found.
    """
    username = os.getenv('POSTGRES_USER')
    if username is None:
        raise RuntimeError("Missing username (env var POSTGRES_USER)")

    password = os.getenv('POSTGRES_PASSWORD')
    if password is None:
        raise RuntimeError("Missing password (env var POSTGRES_PASSWORD)")

    database = os.getenv('POSTGRES_DB')
    if database is None:
        raise RuntimeError("Missing database name (env var POSTGRES_DB)")

    host = os.getenv('POSTGRES_HOST')
    if host is None:
        raise RuntimeError("Missing database name (env var POSTGRES_HOST)")

    url_object = URL.create(
        "postgresql+psycopg2",
        username=username,
        password=password,
        host=host,
        database=database,
    )
    return url_object
