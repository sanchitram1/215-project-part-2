"""
Transform module for ETL pipeline.

Handles data transformation from OLTP schema to OLAP star schema.
Receives raw DataFrames from extract.py and outputs transformed DataFrames
ready for loading into OLAP database.

DIMENSION TABLES:
- users: User dimension with user_id
- content: Content dimension with content_id, likes, upload_time, comments, social_media_data
- places: Place dimension with place_id
- property: Property dimension with property_id

FACT TABLE:
- Central fact table: user_id, content_id, place_id, property_id

See sql/OLAP schema.jpg for visual schema diagram.
"""

import pandas as pd

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_users(raw_users: pd.DataFrame) -> pd.DataFrame:
    """
    Transform users table to OLAP dimension table.

    TODO:
    1. Review raw_users columns and OLAP users dimension schema in sql/schema.sql
    2. Select/rename columns to match OLAP dimension
    3. Handle any data type conversions needed
    4. Remove duplicates if necessary
    5. Add any calculated fields needed

    Args:
        raw_users: DataFrame from OLTP users table (SELECT * result)

    Returns:
        Transformed users dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting users transformation")

        # TODO: Implement transformation logic here
        # Start by inspecting the raw data:
        # logger.debug(f"Raw users columns: {raw_users.columns.tolist()}")
        # logger.debug(f"Raw users shape: {raw_users.shape}")

        transformed_users = raw_users.copy()

        # TODO: Add transformation steps (column selection, renaming, type conversion, etc.)

        logger.info(f"Completed users transformation ({len(transformed_users)} rows)")
        return transformed_users

    except Exception as e:
        raise ValueError(f"Failed to transform users: {e}") from e


def transform_content(raw_content: pd.DataFrame) -> pd.DataFrame:
    """
    Transform content table to OLAP dimension table.

    TODO:
    1. Review raw_content columns and OLAP content dimension schema in sql/schema.sql
    2. Select/rename columns to match OLAP dimension
    3. Parse social_media_data if it's JSON or structured format
    4. Handle any data type conversions needed (timestamps, etc.)
    5. Remove duplicates if necessary

    Args:
        raw_content: DataFrame from OLTP content table (SELECT * result)

    Returns:
        Transformed content dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting content transformation")

        # TODO: Implement transformation logic here
        # Note: content table has large description field - may need special handling

        transformed_content = raw_content.copy()

        # TODO: Add transformation steps (column selection, renaming, type conversion, etc.)

        logger.info(
            f"Completed content transformation ({len(transformed_content)} rows)"
        )
        return transformed_content

    except Exception as e:
        raise ValueError(f"Failed to transform content: {e}") from e


def transform_places(raw_places: pd.DataFrame) -> pd.DataFrame:
    """
    Transform places table to OLAP dimension table.

    TODO:
    1. Review raw_places columns and OLAP places dimension schema in sql/schema.sql
    2. Select/rename columns to match OLAP dimension (place_id is key)
    3. Handle any data type conversions needed
    4. Remove duplicates if necessary

    Args:
        raw_places: DataFrame from OLTP places table (SELECT * result)

    Returns:
        Transformed places dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting places transformation")

        # TODO: Implement transformation logic here

        transformed_places = raw_places.copy()

        # TODO: Add transformation steps (column selection, renaming, type conversion, etc.)

        logger.info(f"Completed places transformation ({len(transformed_places)} rows)")
        return transformed_places

    except Exception as e:
        raise ValueError(f"Failed to transform places: {e}") from e


def transform_property(raw_property: pd.DataFrame) -> pd.DataFrame:
    """
    Transform property table to OLAP dimension table.

    TODO:
    1. Review raw_property columns and OLAP property dimension schema in sql/schema.sql
    2. Select/rename columns to match OLAP dimension (property_id is key)
    3. Handle any data type conversions needed
    4. Remove duplicates if necessary

    Args:
        raw_property: DataFrame from OLTP property_mapping table (SELECT * result)

    Returns:
        Transformed property dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting property transformation")

        # TODO: Implement transformation logic here

        transformed_property = raw_property.copy()

        # TODO: Add transformation steps (column selection, renaming, type conversion, etc.)

        logger.info(
            f"Completed property transformation ({len(transformed_property)} rows)"
        )
        return transformed_property

    except Exception as e:
        raise ValueError(f"Failed to transform property: {e}") from e


def transform_fact_table(
    raw_user_content: pd.DataFrame,
    user_id_map: dict,
    content_id_map: dict,
    place_id_map: dict,
    property_id_map: dict,
) -> pd.DataFrame:
    """
    Transform user_content table to OLAP fact table.

    The fact table is the central table linking all dimensions.

    TODO:
    1. Review raw_user_content columns and fact table schema in sql/schema.sql
    2. Validate that user_id, content_id, place_id, property_id exist in raw data
    3. Use the provided ID maps to ensure referential integrity if needed
    4. Select only the dimension key columns for the fact table
    5. Handle any missing/NULL values appropriately

    Args:
        raw_user_content: DataFrame from OLTP user_content table (SELECT * result)
        user_id_map: Dict mapping raw user IDs to OLAP user IDs (if transformation needed)
        content_id_map: Dict mapping raw content IDs to OLAP content IDs
        place_id_map: Dict mapping raw place IDs to OLAP place IDs
        property_id_map: Dict mapping raw property IDs to OLAP property IDs

    Returns:
        Transformed fact table DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting fact table transformation")

        # TODO: Implement transformation logic here
        # The fact table typically just contains foreign keys to dimensions

        fact_table = raw_user_content.copy()

        # TODO: Add transformation steps (validation, ID mapping, column selection, etc.)

        logger.info(f"Completed fact table transformation ({len(fact_table)} rows)")
        return fact_table

    except Exception as e:
        raise ValueError(f"Failed to transform fact table: {e}") from e


def transform(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Main transformation orchestrator.

    Takes raw OLTP data from extract.py and transforms it into OLAP star schema.
    Returns a dictionary of transformed dimension and fact tables ready for loading.

    TODO:
    1. Call transform_users(), transform_content(), transform_places(), transform_property()
    2. Create ID maps from dimension tables if needed (for referential integrity)
    3. Call transform_fact_table() with the ID maps
    4. Return dictionary with all transformed tables

    Args:
        raw_data: Dictionary from extract_all() with keys:
                  {"users", "content", "places", "property_mapping", "user_content"}

    Returns:
        Dictionary of transformed tables with keys:
        {"users", "content", "places", "property", "fact_table"}
        Each value is a pandas DataFrame.

    Raises:
        ValueError: If any transformation fails
        KeyError: If expected tables are missing from raw_data
    """
    try:
        logger.info("Starting ETL transformation")

        # Validate input
        required_tables = {
            "users",
            "content",
            "places",
            "property_mapping",
            "user_content",
        }
        if not required_tables.issubset(raw_data.keys()):
            missing = required_tables - raw_data.keys()
            raise KeyError(f"Missing required tables: {missing}")

        # TODO: Implement orchestration logic here
        # Once you are done implementing the individual functions above, all you need
        # to do is just incomment the below code, and it should work

        # HOWEVER, NOTE that you will need to specify ID maps
        # The idea is that the OLAP Fact table (interactions) needs foreign keys
        # from the interactions table to users, property, content, and places
        # So, we need to know the IDs ahead of time, which is why we are first
        # transforming the individual tables

        transformed_data = {
            # "users": transform_users(raw_data["users"]),
            # "content": transform_content(raw_data["content"]),
            # "places": transform_places(raw_data["places"]),
            # "property": transform_property(raw_data["property_mapping"]),
            # "fact_table": transform_fact_table(
            #     raw_data["user_content"],
            #     user_id_map={},
            #     content_id_map={},
            #     place_id_map={},
            #     property_id_map={},
            # ),
        }

        logger.info(f"Completed transformation with {len(transformed_data)} tables")
        return transformed_data

    except Exception as e:
        raise ValueError(f"Transformation failed: {e}") from e
