"""
Extract module for ETL pipeline.

Extracts data from OLTP source database and returns DataFrames
for each required table. Uses thread pooling for parallel extraction
to maximize efficiency.

NOTE: Uses SELECT * queries. Schema changes in source database
will change output structure. It's fine for now, but in case the
schema changes frequently, this will break
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from pipeline.database import get_oltp_connection_params
from pipeline.logging_config import get_logger

logger = get_logger(__name__)

# Load environment variables from .env file
load_dotenv()

# Tables to extract from OLTP source
TABLES_TO_EXTRACT = [
    "users",
    "contents",
    "places",
    "property_mapping",
    "user_contents",
]


def extract_table(table_name: str, conn_params: dict) -> tuple[str, pd.DataFrame]:
    """
    Extract all data from a single table.

    Args:
        table_name: Name of table to extract
        conn_params: Dictionary with host, user, password, port, database

    Returns:
        Tuple of (table_name, DataFrame) for result aggregation

    Raises:
        psycopg2.Error: If database connection or query fails
        ValueError: If table extraction returns no data

    TODO: Implement chunked reading for content table (use pd.read_sql_query with chunksize parameter).
          The content table has large description fields that cause slow loading (~5 min).
          Chunked reading would improve memory efficiency.
    """
    try:
        with psycopg2.connect(**conn_params) as conn:
            query = f"SELECT * FROM {table_name};"
            logger.debug(f"Executing query for table '{table_name}': {query}")
            df = pd.read_sql_query(query, conn)

            if df.empty:
                raise ValueError(f"Table '{table_name}' returned no rows")

            logger.info(f"Successfully fetched table '{table_name}' ({len(df)} rows)")
            return (table_name, df)

    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to extract table '{table_name}': {e}") from e


def extract_all() -> dict[str, pd.DataFrame]:
    """
    Extract all required tables from OLTP source in parallel.

    Reads OLTP_DATABASE_URL from environment variables and extracts
    all tables defined in TABLES_TO_EXTRACT using thread pooling
    for maximum efficiency.

    Returns:
        Dictionary mapping table names to DataFrames.
        Example: {"users": df_users, "content": df_content, ...}

    Raises:
        ValueError: If OLTP_DATABASE_URL not set or parsing fails
        psycopg2.Error: If any table extraction fails
    """
    # Get connection parameters from environment
    conn_params = get_oltp_connection_params()

    # Extract tables in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=len(TABLES_TO_EXTRACT)) as executor:
        # Submit all extraction tasks
        futures = {
            executor.submit(extract_table, table, conn_params): table
            for table in TABLES_TO_EXTRACT
        }

        # Collect results as they complete
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                _, df = future.result()
                results[table_name] = df
            except Exception as e:
                raise RuntimeError(f"Error extracting table '{table_name}': {e}") from e

    return results


if __name__ == "__main__":
    data = extract_all()
    for k, v in data.items():
        print(f"{k} has {v.shape} dimension")
