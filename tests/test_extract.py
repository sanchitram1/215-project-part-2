"""
Tests for ETL extract module.

Tests database connection parsing, table extraction, and parallel extraction
with mocked database connections to avoid needing a live database.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Mock psycopg2 before importing extract module to avoid library dependency
sys.modules["psycopg2"] = MagicMock()

from pipeline.database import parse_database_url  # noqa:E402

# Import the module under test after mocking psycopg2
from pipeline.extract import TABLES_TO_EXTRACT, extract_all, extract_table  # noqa: E402


# Create exception classes for mocking
class MockPsycopg2Error(Exception):
    """Mock psycopg2.Error"""

    pass


class MockOperationalError(MockPsycopg2Error):
    """Mock psycopg2.OperationalError"""

    pass


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_database_url():
    """Valid PostgreSQL connection URL."""
    return "postgresql://testuser:testpass@localhost:5432/testdb"


@pytest.fixture
def sample_connection_params():
    """Expected parsed connection parameters."""
    return {
        "host": "localhost",
        "user": "testuser",
        "password": "testpass",
        "port": 5432,
        "database": "testdb",
    }


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for mocking query results."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 15.8],
        }
    )


# ============================================================================
# Tests for parse_database_url
# ============================================================================


class TestParseDatabaseUrl:
    """Test database URL parsing."""

    def test_valid_url_with_all_params(
        self, sample_database_url, sample_connection_params
    ):
        """Parse valid URL with all connection parameters."""
        result = parse_database_url(sample_database_url)
        assert result == sample_connection_params

    def test_valid_url_default_port(self):
        """Parse URL without explicit port (should default to 5432)."""
        url = "postgresql://user:pass@host/mydb"
        result = parse_database_url(url)
        assert result["port"] == 5432
        assert result["host"] == "host"
        assert result["user"] == "user"
        assert result["password"] == "pass"
        assert result["database"] == "mydb"

    def test_invalid_url_format(self):
        """Invalid URL should raise ValueError."""
        with pytest.raises(ValueError, match="URL must have scheme, hostname"):
            parse_database_url("not-a-valid-url")

    def test_empty_url(self):
        """Empty URL should raise ValueError."""
        with pytest.raises(ValueError, match="URL must have scheme, hostname"):
            parse_database_url("")

    def test_url_without_password(self):
        """URL without password should still parse."""
        url = "postgresql://user@localhost/mydb"
        result = parse_database_url(url)
        assert result["user"] == "user"
        assert result["password"] is None
        assert result["host"] == "localhost"


# ============================================================================
# Tests for extract_table
# ============================================================================


class TestExtractTable:
    """Test single table extraction."""

    @patch("pipeline.extract.psycopg2.connect")
    @patch("pipeline.extract.pd.read_sql_query")
    def test_successful_extraction(
        self, mock_read_sql, mock_connect, sample_dataframe, sample_connection_params
    ):
        """Successfully extract a table and return tuple."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_read_sql.return_value = sample_dataframe

        # Execute
        table_name, df = extract_table("users", sample_connection_params)

        # Verify
        assert table_name == "users"
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]
        mock_read_sql.assert_called_once()

    @patch("pipeline.extract.psycopg2.connect")
    @patch("pipeline.extract.pd.read_sql_query")
    def test_extraction_empty_table(
        self, mock_read_sql, mock_connect, sample_connection_params
    ):
        """Empty table should raise ValueError."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_read_sql.return_value = pd.DataFrame()  # Empty DataFrame

        # Execute and verify
        with pytest.raises(ValueError, match="Table 'users' returned no rows"):
            extract_table("users", sample_connection_params)

    @patch("pipeline.extract.psycopg2.connect")
    def test_extraction_database_error(self, mock_connect, sample_connection_params):
        """Database connection error should raise psycopg2.Error."""
        # Setup mocks to raise error
        mock_connect.side_effect = MockOperationalError("Connection refused")

        # Execute and verify
        with pytest.raises(Exception):
            extract_table("users", sample_connection_params)

    @patch("pipeline.extract.psycopg2.connect")
    @patch("pipeline.extract.pd.read_sql_query")
    def test_extraction_query_format(
        self, mock_read_sql, mock_connect, sample_dataframe, sample_connection_params
    ):
        """Verify correct SQL query is executed with configured columns."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_read_sql.return_value = sample_dataframe

        # Execute
        extract_table("contents", sample_connection_params)

        # Verify the query includes table name and only configured columns (not SELECT *)
        call_args = mock_read_sql.call_args
        query = call_args[0][0]
        assert "SELECT" in query
        assert "FROM contents" in query
        # Verify it uses specific columns, not * (SELECT * would be generic)
        assert "id" in query
        assert "title" in query


# ============================================================================
# Tests for extract_all
# ============================================================================


class TestExtractAll:
    """Test parallel extraction of all tables."""

    @patch.dict(
        os.environ, {"OLTP_DATABASE_URL": "postgresql://user:pass@localhost/db"}
    )
    @patch("pipeline.extract.extract_table")
    def test_extract_all_success(self, mock_extract_table):
        """Successfully extract all tables and return dict."""
        # Setup mocks
        mock_extract_table.side_effect = [
            ("users", pd.DataFrame({"id": [1, 2]})),
            ("contents", pd.DataFrame({"id": [1]})),
            ("places", pd.DataFrame({"id": [1, 2, 3]})),
            ("property_mapping", pd.DataFrame({"id": [1]})),
            ("user_contents", pd.DataFrame({"id": [1, 2, 3, 4]})),
            ("content_places", pd.DataFrame({"content_id": [1], "place_id": [1]})),
            ("place_properties", pd.DataFrame({"place_id": [1], "property_id": [1]})),
        ]

        # Execute
        result = extract_all()

        # Verify
        assert isinstance(result, dict)
        assert set(result.keys()) == set(TABLES_TO_EXTRACT)
        assert all(isinstance(df, pd.DataFrame) for df in result.values())
        assert mock_extract_table.call_count == 7

    @patch.dict(os.environ, {}, clear=True)
    def test_extract_all_missing_env_var(self):
        """Missing OLTP_DATABASE_URL should raise ValueError."""
        with pytest.raises(ValueError, match="OLTP_DATABASE_URL not set"):
            extract_all()

    @patch.dict(
        os.environ, {"OLTP_DATABASE_URL": "postgresql://user:pass@localhost/db"}
    )
    @patch("pipeline.extract.extract_table")
    def test_extract_all_partial_failure(self, mock_extract_table):
        """One table extraction failure should propagate error."""
        # Setup mocks - second table fails
        mock_extract_table.side_effect = [
            ("users", pd.DataFrame({"id": [1]})),
            MockOperationalError("Connection lost"),
        ]

        # Execute and verify
        with pytest.raises(RuntimeError, match="Error extracting table"):
            extract_all()

    @patch.dict(
        os.environ, {"OLTP_DATABASE_URL": "postgresql://user:pass@localhost/db"}
    )
    @patch("pipeline.extract.extract_table")
    def test_extract_all_calls_with_correct_params(self, mock_extract_table):
        """extract_all should call extract_table with correct parameters."""
        # Setup mocks
        mock_extract_table.side_effect = [
            (table, pd.DataFrame({"id": [1]})) for table in TABLES_TO_EXTRACT
        ]

        # Execute
        extract_all()

        # Verify all tables were requested
        called_tables = [call[0][0] for call in mock_extract_table.call_args_list]
        assert sorted(called_tables) == sorted(TABLES_TO_EXTRACT)

    @patch.dict(os.environ, {"OLTP_DATABASE_URL": "invalid-url-format"})
    def test_extract_all_invalid_url(self):
        """Invalid database URL should raise ValueError."""
        with pytest.raises(ValueError, match="URL must have scheme, hostname"):
            extract_all()
