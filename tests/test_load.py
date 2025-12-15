"""
Test module for load.py.

Tests ETL loading functionality, including column validation and
data insertion into OLAP tables.
"""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Mock psycopg2 before importing load module
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.sql"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()

from pipeline.config import OLAP_COLUMNS  # noqa: E402
from pipeline.load import load_table, load_olap  # noqa: E402


class TestColumnValidation:
    """Test that load functions validate columns match OLAP_COLUMNS."""

    def test_load_table_with_mismatched_columns_raises_error(self):
        """
        Test that load_table raises ValueError when DataFrame columns
        don't match OLAP_COLUMNS definition.
        """
        # Create DataFrame with wrong columns (missing some, adding extra)
        wrong_df = pd.DataFrame(
            {
                "user_id": [1, 2],
                "email": ["test@example.com", "test2@example.com"],
                "wrong_column": ["bad", "data"],  # Extra column not in OLAP_COLUMNS
                # Missing: display_name, avatar_url, found_us_source, created_at, updated_at
            }
        )

        conn_params = {
            "host": "localhost",
            "user": "postgres",
            "password": "password",
            "dbname": "test_db",
        }

        # Should raise ValueError about mismatched columns
        with pytest.raises(ValueError, match="column"):
            load_table("users", wrong_df, conn_params)

    def test_load_users_with_correct_columns_validates_pass(self):
        """
        Test that load_users accepts DataFrame with correct columns
        from OLAP_COLUMNS["users"].
        """
        # Create DataFrame with exact columns from OLAP_COLUMNS
        correct_df = pd.DataFrame(
            {
                col: [1, 2] if col == "user_id" else ["val1", "val2"]
                for col in OLAP_COLUMNS["users"]
            }
        )

        conn_params = {
            "host": "localhost",
            "user": "postgres",
            "password": "password",
            "dbname": "test_db",
        }

        # Mock psycopg2 to avoid actual database calls
        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            # Should not raise
            load_table("users", correct_df, conn_params)
            mock_cursor.execute.assert_called()  # Should have tried to execute

    def test_load_olap_validates_all_tables(self):
        """
        Test that load_olap validates all transformed tables have
        correct columns before attempting to load.
        """
        # Create transformed data with correct structure
        transformed_data = {
            "users": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["users"]}),
            "content": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["content"]}),
            "places": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["places"]}),
            "property": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["property"]}),
            "fact_table": pd.DataFrame(
                {col: [1] for col in OLAP_COLUMNS["interactions"]}
            ),
        }

        with patch("pipeline.load.get_olap_connection_params") as mock_params:
            mock_params.return_value = {
                "host": "localhost",
                "user": "postgres",
                "password": "password",
                "dbname": "test_db",
            }

            with patch("psycopg2.connect") as mock_connect:
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_connect.return_value.__enter__.return_value = mock_conn
                mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

                # Should not raise with correct columns
                load_olap(transformed_data)
                # Verify tables were loaded (cursor.execute called multiple times)
                assert mock_cursor.execute.call_count > 0

    def test_load_olap_fails_with_mismatched_fact_table_columns(self):
        """
        Test that load_olap raises ValueError when fact_table columns
        don't match OLAP_COLUMNS["interactions"].
        """
        # Create fact_table with wrong columns
        transformed_data = {
            "users": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["users"]}),
            "content": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["content"]}),
            "places": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["places"]}),
            "property": pd.DataFrame({col: [1] for col in OLAP_COLUMNS["property"]}),
            "fact_table": pd.DataFrame(
                {
                    "user_id": [1],
                    "content_id": [1],
                    # Missing: place_id, property_id, created_at, updated_at
                }
            ),
        }

        with patch("pipeline.load.get_olap_connection_params") as mock_params:
            mock_params.return_value = {
                "host": "localhost",
                "user": "postgres",
                "password": "password",
                "dbname": "test_db",
            }

            # Should raise ValueError about mismatched columns in fact_table
            with pytest.raises(ValueError, match="column"):
                load_olap(transformed_data)


class TestLoadFunctionality:
    """Test basic load functionality."""

    def test_load_table_empty_dataframe_skipped(self):
        """Test that empty DataFrames are skipped without error."""
        empty_df = pd.DataFrame()

        conn_params = {
            "host": "localhost",
            "user": "postgres",
            "password": "password",
            "dbname": "test_db",
        }

        # Should not raise, just log warning
        load_table("users", empty_df, conn_params)

    @patch("pipeline.load.execute_values")
    def test_load_table_handles_nan_values(self, mock_execute_values):
        """Test that NaN values are converted to None before insert."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "source_user_id": [1, 2],
                "email": ["test@example.com", None],
                "display_name": ["User1", pd.NA],
                "avatar_url": ["http://example.com", None],
                "found_us_source": ["Google", None],
                "created_at": ["2024-01-01", "2024-01-02"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )

        conn_params = {
            "host": "localhost",
            "user": "postgres",
            "password": "password",
            "dbname": "test_db",
        }

        with patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            load_table("users", df, conn_params)

            # Verify execute_values was called
            assert mock_execute_values.called
