"""
Database connection utilities for ETL pipeline.

Provides shared database connection and URL parsing functionality
used by extract.py, load.py, and other modules.
"""

import os
from urllib.parse import urlparse

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def parse_database_url(url: str) -> dict:
    """
    Parse PostgreSQL connection URL into psycopg2 params.

    Args:
        url: PostgreSQL connection URL (postgresql://user:password@host:port/dbname)

    Returns:
        Dictionary with keys: host, user, password, port, database

    Raises:
        ValueError: If URL format is invalid
    """
    try:
        logger.debug("Parsing database URL")
        parsed = urlparse(url)

        # Validate URL has required components
        if not parsed.scheme or not parsed.hostname or not parsed.path:
            raise ValueError("URL must have scheme, hostname, and database name")

        return {
            "host": parsed.hostname,
            "user": parsed.username,
            "password": parsed.password,
            "port": parsed.port or 5432,
            "database": parsed.path.lstrip("/"),
        }
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Invalid database URL format: {e}")


def get_oltp_connection_params() -> dict:
    """
    Get OLTP database connection parameters from environment.

    Reads OLTP_DATABASE_URL from environment variables and parses it.

    Returns:
        Dictionary with keys: host, user, password, port, database

    Raises:
        ValueError: If OLTP_DATABASE_URL not set or parsing fails
    """
    db_url = os.getenv("OLTP_DATABASE_URL")
    if not db_url:
        raise ValueError(
            "OLTP_DATABASE_URL not set in environment. "
            "Check .env file and load it before running."
        )

    return parse_database_url(db_url)


def get_olap_connection_params() -> dict:
    """
    Get OLAP database connection parameters from environment.

    Reads OLAP_DATABASE_URL from environment variables and parses it.

    Returns:
        Dictionary with keys: host, user, password, port, database

    Raises:
        ValueError: If OLAP_DATABASE_URL not set or parsing fails
    """
    db_url = os.getenv("OLAP_DATABASE_URL")
    if not db_url:
        raise ValueError(
            "OLAP_DATABASE_URL not set in environment. "
            "Check .env file and load it before running."
        )

    return parse_database_url(db_url)
