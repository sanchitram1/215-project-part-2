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

See sql/OLAP_schema.jpg for visual schema diagram.
"""

import pandas as pd

from pipeline.config import OLAP_COLUMNS, OLTP_COLUMNS
from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def transform_users(raw_users: pd.DataFrame) -> pd.DataFrame:
    """
    Transform users table to OLAP dimension table.

    Renames OLTP 'id' column to 'source_user_id' and selects required columns
    matching the OLAP schema. Removes duplicates if necessary.

    Args:
        raw_users: DataFrame from OLTP users table

    Returns:
        Transformed users dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting users transformation")
        logger.debug(f"Raw users columns: {raw_users.columns.tolist()}")
        logger.debug(f"Raw users shape: {raw_users.shape}")

        # Validate required columns
        required_columns = set(OLTP_COLUMNS["users"])
        if not required_columns.issubset(raw_users.columns):
            missing = required_columns - set(raw_users.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Rename 'id' to 'source_user_id' to match OLAP schema
        transformed_users = raw_users.copy()
        transformed_users = transformed_users.rename(columns={"id": "source_user_id"})

        # Select only OLAP schema columns (excluding 'id' which we'll add)
        olap_columns = [col for col in OLAP_COLUMNS["users"] if col != "id"]
        transformed_users = transformed_users[olap_columns]

        # Remove duplicates if any (keep first occurrence)
        transformed_users = transformed_users.drop_duplicates(
            subset=["source_user_id"], keep="first"
        )

        # Pre-generate surrogate IDs (starting from 1)
        transformed_users.insert(0, "id", range(1, len(transformed_users) + 1))

        logger.info(f"Completed users transformation ({len(transformed_users)} rows)")
        return transformed_users

    except Exception as e:
        raise ValueError(f"Failed to transform users: {e}") from e


def transform_content(raw_content: pd.DataFrame) -> pd.DataFrame:
    """
    Transform content table to OLAP dimension table.

    Renames OLTP 'id' column to 'source_content_id' and selects required columns
    matching the OLAP schema. Maps OLTP columns to OLAP columns where names differ.
    Note: OLTP schema doesn't include 'platform' or 'platform_id' fields.

    Args:
        raw_content: DataFrame from OLTP contents table

    Returns:
        Transformed content dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting content transformation")
        logger.debug(f"Raw content columns: {raw_content.columns.tolist()}")
        logger.debug(f"Raw content shape: {raw_content.shape}")

        # Validate required OLTP columns
        required_columns = set(OLTP_COLUMNS["contents"])
        if not required_columns.issubset(raw_content.columns):
            missing = required_columns - set(raw_content.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Rename 'id' to 'source_content_id' to match OLAP schema
        transformed_content = raw_content.copy()
        transformed_content = transformed_content.rename(columns={"id": "source_content_id"})

        # Map OLTP preview_image_url to OLAP thumbnail_url
        if "preview_image_url" in transformed_content.columns:
            transformed_content["thumbnail_url"] = transformed_content.get(
                "preview_image_url"
            )

        # Add placeholder columns for platform and platform_id (not in OLTP)
        if "platform" not in transformed_content.columns:
            transformed_content["platform"] = None
        if "platform_id" not in transformed_content.columns:
            transformed_content["platform_id"] = None

        # Select only OLAP schema columns (excluding 'id' which we'll add)
        olap_columns = [col for col in OLAP_COLUMNS["content"] if col != "id"]
        transformed_content = transformed_content[olap_columns]

        # Remove duplicates if any (keep first occurrence)
        transformed_content = transformed_content.drop_duplicates(
            subset=["source_content_id"], keep="first"
        )

        # Pre-generate surrogate IDs (starting from 1)
        transformed_content.insert(0, "id", range(1, len(transformed_content) + 1))

        logger.info(
            f"Completed content transformation ({len(transformed_content)} rows)"
        )
        return transformed_content

    except Exception as e:
        raise ValueError(f"Failed to transform content: {e}") from e


def transform_places(raw_places: pd.DataFrame) -> pd.DataFrame:
    """
    Transform places table to OLAP dimension table.

    Renames OLTP 'id' column to 'source_place_id' and selects required columns
    matching the OLAP schema.

    Args:
        raw_places: DataFrame from OLTP places table

    Returns:
        Transformed places dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting places transformation")
        logger.debug(f"Raw places columns: {raw_places.columns.tolist()}")
        logger.debug(f"Raw places shape: {raw_places.shape}")

        # Validate required OLTP columns
        required_columns = set(OLTP_COLUMNS["places"])
        if not required_columns.issubset(raw_places.columns):
            missing = required_columns - set(raw_places.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Rename 'id' to 'source_place_id' to match OLAP schema
        transformed_places = raw_places.copy()
        transformed_places = transformed_places.rename(columns={"id": "source_place_id"})

        # Select only OLAP schema columns (excluding 'id' which we'll add)
        olap_columns = [col for col in OLAP_COLUMNS["places"] if col != "id"]
        transformed_places = transformed_places[olap_columns]

        # Remove duplicates if any (keep first occurrence)
        transformed_places = transformed_places.drop_duplicates(
            subset=["source_place_id"], keep="first"
        )

        # Pre-generate surrogate IDs (starting from 1)
        transformed_places.insert(0, "id", range(1, len(transformed_places) + 1))

        logger.info(f"Completed places transformation ({len(transformed_places)} rows)")
        return transformed_places

    except Exception as e:
        raise ValueError(f"Failed to transform places: {e}") from e


def transform_property(raw_property: pd.DataFrame) -> pd.DataFrame:
    """
    Transform property table to OLAP dimension table.

    Renames OLTP 'id' column to 'source_property_id' and maps column names to match OLAP schema
    (e.g., english_display_name -> english_name, zhtw_display_name -> zhtw_name).

    Args:
        raw_property: DataFrame from OLTP property_mapping table

    Returns:
        Transformed property dimension DataFrame ready for loading

    Raises:
        ValueError: If required columns are missing or transformation fails
    """
    try:
        logger.info("Starting property transformation")
        logger.debug(f"Raw property columns: {raw_property.columns.tolist()}")
        logger.debug(f"Raw property shape: {raw_property.shape}")

        # Validate required OLTP columns
        required_columns = set(OLTP_COLUMNS["property_mapping"])
        if not required_columns.issubset(raw_property.columns):
            missing = required_columns - set(raw_property.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Rename 'id' to 'source_property_id' to match OLAP schema
        transformed_property = raw_property.copy()
        transformed_property = transformed_property.rename(
            columns={
                "id": "source_property_id",
                "english_display_name": "english_name",
                "zhtw_display_name": "zhtw_name",
            }
        )

        # Select only OLAP schema columns (excluding 'id' which we'll add)
        olap_columns = [col for col in OLAP_COLUMNS["property"] if col != "id"]
        transformed_property = transformed_property[olap_columns]

        # Remove duplicates if any (keep first occurrence)
        transformed_property = transformed_property.drop_duplicates(
            subset=["source_property_id"], keep="first"
        )

        # Pre-generate surrogate IDs (starting from 1)
        transformed_property.insert(0, "id", range(1, len(transformed_property) + 1))

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
    It connects users to content to places to properties through a
    star schema with foreign key references.

    Maps raw IDs to OLAP IDs using the provided maps and creates the
    interaction records.

    Args:
        raw_user_content: DataFrame from OLTP user_contents table
        user_id_map: Dict mapping raw user IDs to OLAP user IDs
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
        logger.debug(f"Raw user_content columns: {raw_user_content.columns.tolist()}")
        logger.debug(f"Raw user_content shape: {raw_user_content.shape}")

        # Validate required columns from OLTP
        required_columns = {"user_id", "content_id"}
        if not required_columns.issubset(raw_user_content.columns):
            missing = required_columns - set(raw_user_content.columns)
            raise ValueError(f"Missing required columns: {missing}")

        fact_table = raw_user_content.copy()

        # Store source IDs before mapping
        fact_table["source_user_id"] = fact_table["user_id"]
        fact_table["source_content_id"] = fact_table["content_id"]

        # Map OLTP IDs to OLAP IDs (these are the FK values)
        fact_table["user_id"] = fact_table["user_id"].map(user_id_map)
        fact_table["content_id"] = fact_table["content_id"].map(content_id_map)

        # Note: place_id and property_id may need to be filled from junction tables
        # For now, use the maps if provided, otherwise set to NULL
        fact_table["source_place_id"] = None
        fact_table["source_property_id"] = None

        if place_id_map:
            fact_table["place_id"] = fact_table.get("place_id", pd.Series()).map(
                place_id_map
            )
        else:
            fact_table["place_id"] = None

        if property_id_map:
            fact_table["property_id"] = fact_table.get("property_id", pd.Series()).map(
                property_id_map
            )
        else:
            fact_table["property_id"] = None

        # Select only OLAP fact table columns
        olap_columns = OLAP_COLUMNS["interactions"]
        fact_table = fact_table[olap_columns]

        # Remove rows where user_id or content_id is NULL (failed mapping)
        fact_table = fact_table.dropna(subset=["user_id", "content_id"])

        logger.info(f"Completed fact table transformation ({len(fact_table)} rows)")
        return fact_table

    except Exception as e:
        raise ValueError(f"Failed to transform fact table: {e}") from e


def transform(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Main transformation orchestrator.

    Takes raw OLTP data from extract.py and transforms it into OLAP star schema.
    Returns a dictionary of transformed dimension and fact tables ready for loading.

    Args:
        raw_data: Dictionary from extract_all() with keys:
                  {"users", "contents", "places", "property_mapping", "user_contents"}

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
            "contents",
            "places",
            "property_mapping",
            "user_contents",
        }
        if not required_tables.issubset(raw_data.keys()):
            missing = required_tables - raw_data.keys()
            raise KeyError(f"Missing required tables: {missing}")

        # Transform dimension tables
        logger.info("Transforming dimension tables")
        users_df = transform_users(raw_data["users"])
        content_df = transform_content(raw_data["contents"])
        places_df = transform_places(raw_data["places"])
        property_df = transform_property(raw_data["property_mapping"])

        # Create ID maps for referential integrity in fact table
        # Maps OLTP IDs to pre-generated OLAP surrogate IDs
        user_id_map = dict(zip(raw_data["users"]["id"], users_df["id"]))
        content_id_map = dict(zip(raw_data["contents"]["id"], content_df["id"]))
        place_id_map = dict(zip(raw_data["places"]["id"], places_df["id"]))
        property_id_map = dict(
            zip(raw_data["property_mapping"]["id"], property_df["id"])
        )

        logger.info("ID maps created for referential integrity")

        # Transform fact table
        fact_table_df = transform_fact_table(
            raw_data["user_contents"],
            user_id_map=user_id_map,
            content_id_map=content_id_map,
            place_id_map=place_id_map,
            property_id_map=property_id_map,
        )

        transformed_data = {
            "users": users_df,
            "content": content_df,
            "places": places_df,
            "property": property_df,
            "fact_table": fact_table_df,
        }

        logger.info(f"Completed transformation with {len(transformed_data)} tables")
        return transformed_data

    except Exception as e:
        raise ValueError(f"Transformation failed: {e}") from e
