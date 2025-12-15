"""
Test schema alignment between config.py and SQL schema files.

Validates that:
1. OLAP_COLUMNS in config.py matches columns in OLAP_schema.sql
2. OLTP_COLUMNS in config.py matches columns in OLTP_schema.sql
"""

import re
from pathlib import Path

import pytest

from pipeline.config import OLAP_COLUMNS, OLTP_COLUMNS


def extract_sql_columns(sql_content: str, table_name: str) -> set[str]:
    """
    Extract column names from a CREATE TABLE statement in SQL.

    Args:
        sql_content: Full SQL file content
        table_name: Name of table to extract columns from

    Returns:
        Set of column names (lowercase)
    """
    # Find the CREATE TABLE statement for this table
    pattern = rf"CREATE TABLE\s+(?:public\.)?{table_name}\s*\("
    match = re.search(pattern, sql_content, re.IGNORECASE)

    if not match:
        raise ValueError(f"Table '{table_name}' not found in SQL")

    # Find where the table definition starts
    start_pos = match.end()

    # Find matching closing parenthesis
    paren_count = 1
    end_pos = start_pos
    while paren_count > 0 and end_pos < len(sql_content):
        if sql_content[end_pos] == "(":
            paren_count += 1
        elif sql_content[end_pos] == ")":
            paren_count -= 1
        end_pos += 1

    table_def = sql_content[start_pos : end_pos - 1]

    # Extract column definitions
    columns = set()
    lines = table_def.split("\n")

    for line in lines:
        line = line.strip()
        if not line or line.startswith("--"):
            continue

        # Skip lines that are purely constraint definitions
        line_upper = line.upper()
        if line_upper.startswith(
            (
                "CONSTRAINT",
                "PRIMARY KEY",
                "FOREIGN KEY",
                "CHECK",
            )
        ):
            continue

        # For lines that might contain both column definition and type
        # e.g., "rating DECIMAL(2, 1)," - extract just the column name
        # Remove trailing comma and everything after it
        if "," in line:
            line = line.split(",")[0].strip()

        # Extract column name (first token, may be quoted)
        # e.g. from "rating DECIMAL(2, 1)" -> take "rating"
        tokens = line.split()
        if tokens:
            col_name = tokens[0].strip('"').lower()
            # Only add if it looks like a column name (alphanumeric + underscore)
            if (
                col_name
                and not col_name.startswith("'")
                and col_name != ")"
                and col_name.replace("_", "").isalnum()
            ):
                columns.add(col_name)

    return columns


class TestOLAPSchemaAlignment:
    """Test alignment between OLAP_COLUMNS config and OLAP_schema.sql."""

    @pytest.fixture
    def olap_schema_content(self):
        """Load OLAP schema SQL file."""
        schema_path = Path(__file__).parent.parent / "sql" / "OLAP_schema.sql"
        return schema_path.read_text()

    def test_users_table_alignment(self, olap_schema_content):
        """Verify users table columns match OLAP_COLUMNS config."""
        sql_columns = extract_sql_columns(olap_schema_content, "users")
        config_columns = set(OLAP_COLUMNS["users"])

        # SQL has 'id' (auto-generated PK) which we don't load, and 'source_user_id'
        # Config should have all user-loadable columns
        expected_in_sql = {"id", "source_user_id"} | config_columns
        assert sql_columns == expected_in_sql, (
            f"Mismatch in users table.\nSQL: {sql_columns}\nExpected: {expected_in_sql}\nConfig missing: {expected_in_sql - sql_columns}\nExtra in SQL: {sql_columns - expected_in_sql}"
        )

    def test_content_table_alignment(self, olap_schema_content):
        """Verify content table columns match OLAP_COLUMNS config."""
        sql_columns = extract_sql_columns(olap_schema_content, "content")
        config_columns = set(OLAP_COLUMNS["content"])

        # SQL has 'id' (auto-generated PK) which we don't load
        expected_in_sql = {"id"} | config_columns
        assert sql_columns == expected_in_sql, (
            f"Mismatch in content table.\nSQL: {sql_columns}\nExpected: {expected_in_sql}\nConfig missing: {expected_in_sql - sql_columns}\nExtra in SQL: {sql_columns - expected_in_sql}"
        )

    def test_places_table_alignment(self, olap_schema_content):
        """Verify places table columns match OLAP_COLUMNS config."""
        sql_columns = extract_sql_columns(olap_schema_content, "places")
        config_columns = set(OLAP_COLUMNS["places"])

        # SQL has 'id' (auto-generated PK) which we don't load
        expected_in_sql = {"id"} | config_columns
        assert sql_columns == expected_in_sql, (
            f"Mismatch in places table.\nSQL: {sql_columns}\nExpected: {expected_in_sql}\nConfig missing: {expected_in_sql - sql_columns}\nExtra in SQL: {sql_columns - expected_in_sql}"
        )

    def test_property_table_alignment(self, olap_schema_content):
        """Verify property table columns match OLAP_COLUMNS config."""
        sql_columns = extract_sql_columns(olap_schema_content, "property")
        config_columns = set(OLAP_COLUMNS["property"])

        # SQL has 'id' (auto-generated PK) which we don't load
        expected_in_sql = {"id"} | config_columns
        assert sql_columns == expected_in_sql, (
            f"Mismatch in property table.\nSQL: {sql_columns}\nExpected: {expected_in_sql}\nConfig missing: {expected_in_sql - sql_columns}\nExtra in SQL: {sql_columns - expected_in_sql}"
        )

    def test_interactions_table_alignment(self, olap_schema_content):
        """Verify interactions fact table columns match OLAP_COLUMNS config."""
        sql_columns = extract_sql_columns(olap_schema_content, "interactions")
        config_columns = set(OLAP_COLUMNS["interactions"])

        # interactions table has no auto-generated id, config columns should match
        assert sql_columns == config_columns, (
            f"Mismatch in interactions table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )


class TestOLTPSchemaAlignment:
    """Test alignment between OLTP_COLUMNS config and OLTP_schema.sql."""

    @pytest.fixture
    def oltp_schema_content(self):
        """Load OLTP schema SQL file."""
        schema_path = Path(__file__).parent.parent / "sql" / "OLTP_schema.sql"
        return schema_path.read_text()

    def test_users_table_alignment(self, oltp_schema_content):
        """Verify users table columns match OLTP_COLUMNS config."""
        sql_columns = extract_sql_columns(oltp_schema_content, "users")
        config_columns = set(OLTP_COLUMNS["users"])

        assert sql_columns == config_columns, (
            f"Mismatch in users table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in config: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )

    def test_contents_table_alignment(self, oltp_schema_content):
        """Verify contents table columns match OLTP_COLUMNS config."""
        sql_columns = extract_sql_columns(oltp_schema_content, "contents")
        config_columns = set(OLTP_COLUMNS["contents"])

        assert sql_columns == config_columns, (
            f"Mismatch in contents table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in config: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )

    def test_places_table_alignment(self, oltp_schema_content):
        """Verify places table columns match OLTP_COLUMNS config."""
        sql_columns = extract_sql_columns(oltp_schema_content, "places")
        config_columns = set(OLTP_COLUMNS["places"])

        assert sql_columns == config_columns, (
            f"Mismatch in places table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in config: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )

    def test_property_mapping_table_alignment(self, oltp_schema_content):
        """Verify property_mapping table columns match OLTP_COLUMNS config."""
        sql_columns = extract_sql_columns(oltp_schema_content, "property_mapping")
        config_columns = set(OLTP_COLUMNS["property_mapping"])

        assert sql_columns == config_columns, (
            f"Mismatch in property_mapping table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in config: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )

    def test_user_contents_table_alignment(self, oltp_schema_content):
        """Verify user_contents table columns match OLTP_COLUMNS config."""
        sql_columns = extract_sql_columns(oltp_schema_content, "user_contents")
        config_columns = set(OLTP_COLUMNS["user_contents"])

        assert sql_columns == config_columns, (
            f"Mismatch in user_contents table.\nSQL: {sql_columns}\nConfig: {config_columns}\nConfig missing: {config_columns - sql_columns}\nExtra in config: {config_columns - sql_columns}\nExtra in SQL: {sql_columns - config_columns}"
        )
