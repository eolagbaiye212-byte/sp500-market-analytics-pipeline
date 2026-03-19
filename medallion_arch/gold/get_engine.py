# This will create a function that estblishes a connection with PostgreSQL for loading the dataframes into the database as tables

# Import required libraries
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)


def get_engine():
    # Get the user, password, host, port, and db for the given environment
    user = os.environ.get('PROJECT_DB_USER')
    password = os.environ.get('PROJECT_DB_PASSWORD')
    host = os.environ.get('PROJECT_DB_HOST')
    port = os.environ.get('PROJECT_DB_PORT')
    db = os.environ.get('PROJECT_DB_NAME')

    if not password:
        raise ValueError('PGPASSWORD environment variable not set')
    
    # URL encode special characters in password so the engine can parse the string correctly (otherwise, special characters would break the connection string)
    password = quote_plus(password)
    
    logger.info(f"Connecting as user: {user} to host {host}:{port}/{db}")

    # Get engine to establish connection to PostgreSQL database
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    logger.info("Engine for Connection to PostgreSQL Database Retrieved")

    return engine