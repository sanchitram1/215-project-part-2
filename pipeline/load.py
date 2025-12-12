"""
Load module for ETL pipeline.

Handles loading transformed OLAP data into the OLAP database.
Receives transformed DataFrames from transform.py and loads them
into the star schema tables.

OLAP DATABASE TABLES:
- Dimension tables: users, content, places, property
- Fact table: Central fact table linking all dimensions

See sql/schema.sql for complete schema definition and table structures.
See sql/OLAP schema.jpg for visual schema diagram.
"""

import pandas as pd
from dotenv import load_dotenv

from pipeline.database import get_olap_connection_params
from pipeline.logging_config import get_logger

logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()


def load_table(table_name: str, df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load a single dimension/fact table into OLAP database.

    TODO:
    1. Establish connection to OLAP database using connection_params
    2. Clear existing data from table (truncate) or check for duplicates
    3. Insert DataFrame rows into the table
    4. Verify row count matches expected
    5. Log success with row count

    Args:
        table_name: Name of OLAP table to load (e.g., "users", "content", "fact_table")
        df: Transformed DataFrame ready to load
        connection_params: Dict with host, user, password, port, database

    Raises:
        ValueError: If table doesn't exist or load fails
        psycopg2.Error: If database operation fails
    """
    try:
        logger.info(f"Starting load for table '{table_name}'")

        # TODO: Implement loading logic here
        # Hint: Use df.to_sql() or iterate rows with psycopg2
        # Be sure to handle data type conversions (e.g., datetime, UUID)

        logger.info(f"Successfully loaded table '{table_name}' ({len(df)} rows)")

    except Exception as e:
        raise ValueError(f"Failed to load table '{table_name}': {e}") from e


def load_users(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load users dimension table.

    TODO:
    1. Call load_table() with table_name="users"
    2. Or implement custom logic if needed (e.g., upsert instead of truncate/insert)

    Args:
        df: Transformed users DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading users dimension table")
        # TODO: Implement users-specific loading logic
        load_table("users", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load users: {e}") from e


def load_content(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load content dimension table.

    TODO:
    1. Call load_table() with table_name="content"
    2. Handle large description field appropriately

    Args:
        df: Transformed content DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading content dimension table")
        # TODO: Implement content-specific loading logic
        load_table("content", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load content: {e}") from e


def load_places(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load places dimension table.

    TODO:
    1. Call load_table() with table_name="places"

    Args:
        df: Transformed places DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading places dimension table")
        # TODO: Implement places-specific loading logic
        load_table("places", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load places: {e}") from e


def load_property(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load property dimension table.

    TODO:
    1. Call load_table() with table_name="property"

    Args:
        df: Transformed property DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading property dimension table")
        # TODO: Implement property-specific loading logic
        load_table("property", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load property: {e}") from e


def load_fact_table(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load central fact table.

    TODO:
    1. Call load_table() with table_name="fact_table" (or whatever your fact table is named)
    2. Validate foreign key constraints (all user_id, content_id, etc. exist in dimensions)

    Args:
        df: Transformed fact table DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails or foreign key constraints violated
    """
    try:
        logger.info("Loading fact table")
        # TODO: Implement fact table-specific loading logic
        load_table("fact_table", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load fact table: {e}") from e


def load_olap(transformed_data: dict[str, pd.DataFrame]) -> None:
    """
    Main load orchestrator.

    Takes transformed DataFrames from transform.py and loads them
    into the OLAP database star schema.

    TODO:
    1. Get OLAP connection parameters via _get_olap_connection_params()
    2. Validate that all expected tables are in transformed_data
    3. Call individual load_* functions for each dimension table
    4. Call load_fact_table() last (after all dimensions are loaded)
    5. Log overall completion

    Args:
        transformed_data: Dictionary from transform() with keys:
                         {"users", "content", "places", "property", "fact_table"}
                         Each value is a pandas DataFrame

    Raises:
        ValueError: If any load fails
        KeyError: If expected tables are missing from transformed_data
    """
    try:
        logger.info("Starting OLAP load phase")

        # Validate input
        required_tables = {"users", "content", "places", "property", "fact_table"}
        if not required_tables.issubset(transformed_data.keys()):
            missing = required_tables - transformed_data.keys()
            raise KeyError(f"Missing required tables for loading: {missing}")

        # Get connection parameters
        conn_params = get_olap_connection_params()

        # TODO: Implement loading orchestration
        # 1. Load dimension tables first (order may matter for constraints)
        # 2. Load fact table last
        # Example:
        # load_users(transformed_data["users"], conn_params)
        # load_content(transformed_data["content"], conn_params)
        # load_places(transformed_data["places"], conn_params)
        # load_property(transformed_data["property"], conn_params)
        # load_fact_table(transformed_data["fact_table"], conn_params)

        logger.info("Completed OLAP load phase")

    except Exception as e:
        raise ValueError(f"OLAP load failed: {e}") from e
