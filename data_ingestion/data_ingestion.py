"""Data Ingestion Module

This module provides functionalities for data ingestion, allowing users to connect to a database, execute queries, and create DataFrames. Additionally, it supports importing CSV files from URLs into DataFrames, ensuring data integrity.

This module contains the following functions:
    * create_db_engine(db_path) - Returns a connection engine for a specified database path.
    * query_data(engine, sql_query) - Executes a SQL query using the provided engine and returns the result as a DataFrame.
    * read_from_web_CSV(URL) - Downloads and imports a CSV file from a specified URL into a DataFrame.

Dependencies:
    * sqlalchemy
    * pandas
    * logging
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

### START FUNCTION

def create_db_engine(db_path):
    """
    Creates and returns a SQLAlchemy engine for a specified database path.

    Parameters:
    - db_path (str): The database path.

    Returns:
    - engine: The SQLAlchemy engine object.

    Example:
    >>> engine = create_db_engine('sqlite:///Maji_Ndogo_farm_survey_small.db')
    """

    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    Executes a SQL query using the provided engine and returns the result as a DataFrame.

    Parameters:
    - engine: The SQLAlchemy engine object.
    - sql_query (str): The SQL query to be executed.

    Returns:
    - pd.DataFrame: The DataFrame containing the query result.

    Example:
    >>> sql_query = "SELECT * FROM geographic_features LEFT JOIN weather_features USING (Field_ID) LEFT JOIN soil_and_crop_features USING (Field_ID) LEFT JOIN farm_management_features USING (Field_ID)"
    >>> data_frame = query_data(engine, sql_query)
    """

    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Downloads and imports a CSV file from a specified URL into a DataFrame.

    Parameters:
    - URL (str): The URL of the CSV file.

    Returns:
    - pd.DataFrame: The DataFrame containing the CSV data.

    Example:
    >>> weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
    >>> weather_data_frame = read_from_web_CSV(weather_data_URL)
    """
    
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION