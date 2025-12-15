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
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from pipeline.database import get_olap_connection_params
from pipeline.logging_config import get_logger

logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()


def load_table(table_name: str, df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load a single dimension/fact table into OLAP database.

    Args:
        table_name: Name of OLAP table to load (e.g., "users", "content", "fact_table")
        df: Transformed DataFrame ready to load
        connection_params: Dict with host, user, password, port, database

    Raises:
        ValueError: If table doesn't exist or load fails
        psycopg2.Error: If database operation fails
    """
    if df.empty:
        logger.warning(f"DataFrame for '{table_name}' is empty, skipping load")
        return

    try:
        logger.info(f"Starting load for table '{table_name}'")

        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Clear existing data (CASCADE handles FK constraints)
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {} CASCADE").format(
                        sql.Identifier(table_name)
                    )
                )
                logger.debug(f"Truncated table '{table_name}'")

                # Prepare column names
                columns = df.columns.tolist()
                col_names = sql.SQL(", ").join([sql.Identifier(c) for c in columns])

                # Convert DataFrame to list of tuples, handling NaN -> None
                data = [
                    tuple(None if pd.isna(v) else v for v in row)
                    for row in df.itertuples(index=False, name=None)
                ]

                # Bulk insert using execute_values for performance
                query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(table_name),
                    col_names
                )
                execute_values(cur, query, data, page_size=1000)

            conn.commit()

        logger.info(f"Successfully loaded table '{table_name}' ({len(df)} rows)")

    except Exception as e:
        raise ValueError(f"Failed to load table '{table_name}': {e}") from e


def load_users(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load users dimension table.

    Args:
        df: Transformed users DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading users dimension table")
        load_table("users", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load users: {e}") from e


def load_content(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load content dimension table.

    Args:
        df: Transformed content DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading content dimension table")
        load_table("content", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load content: {e}") from e


def load_places(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load places dimension table.

    Args:
        df: Transformed places DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading places dimension table")
        load_table("places", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load places: {e}") from e


def load_property(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load property dimension table.

    Args:
        df: Transformed property DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails
    """
    try:
        logger.info("Loading property dimension table")
        load_table("property", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load property: {e}") from e


def load_fact_table(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load central fact table.

    Args:
        df: Transformed fact table DataFrame
        connection_params: OLAP database connection parameters

    Raises:
        ValueError: If load fails or foreign key constraints violated
    """
    try:
        logger.info("Loading fact table")
        load_table("fact_table", df, connection_params)
    except Exception as e:
        raise ValueError(f"Failed to load fact table: {e}") from e


def load_olap(transformed_data: dict[str, pd.DataFrame]) -> None:
    """
    Main load orchestrator.

    Takes transformed DataFrames from transform.py and loads them
    into the OLAP database star schema.

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

        # Load dimension tables first (order doesn't matter among dimensions)
        load_users(transformed_data["users"], conn_params)
        load_content(transformed_data["content"], conn_params)
        load_places(transformed_data["places"], conn_params)
        load_property(transformed_data["property"], conn_params)

        # Load fact table last (has FK references to all dimensions)
        load_fact_table(transformed_data["fact_table"], conn_params)

        logger.info("Completed OLAP load phase")

    except Exception as e:
        raise ValueError(f"OLAP load failed: {e}") from e