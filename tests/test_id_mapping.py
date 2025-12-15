"""
Tests for ETL ID mapping and field validation.

Validates that source IDs are properly preserved and mapped to surrogate keys,
and that field structure validation occurs gracefully during the load step
with proper error logging.
"""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Mock psycopg2 before importing modules
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.sql"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_users_df():
    """Sample users DataFrame from OLTP source."""
    return pd.DataFrame(
        {
            "id": [101, 102, 103],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "display_name": ["Alice", "Bob", "Charlie"],
            "first_name": ["Alice", "Bob", "Charlie"],
            "last_name": ["Smith", "Jones", "Brown"],
            "avatar_url": ["url1", "url2", "url3"],
            "gender": ["F", "M", "M"],
            "provider": ["google", "github", "google"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "found_us_source": ["search", "referral", "search"],
        }
    )


@pytest.fixture
def sample_content_df():
    """Sample content DataFrame from OLTP source."""
    return pd.DataFrame(
        {
            "id": [201, 202, 203],
            "url": [
                "http://example.com/1",
                "http://example.com/2",
                "http://example.com/3",
            ],
            "html": ["<html>1</html>", "<html>2</html>", "<html>3</html>"],
            "title": ["Article 1", "Article 2", "Article 3"],
            "description": ["Desc 1", "Desc 2", "Desc 3"],
            "site_name": ["Site A", "Site B", "Site C"],
            "icon_url": ["icon1", "icon2", "icon3"],
            "preview_image_url": ["img1", "img2", "img3"],
            "status": ["active", "active", "active"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )


@pytest.fixture
def sample_places_df():
    """Sample places DataFrame from OLTP source."""
    return pd.DataFrame(
        {
            "id": [301, 302, 303],
            "google_maps_id": ["maps1", "maps2", "maps3"],
            "latitude": [40.7128, 34.0522, 51.5074],
            "longitude": [-74.0060, -118.2437, -0.1278],
            "english_display_name": ["New York", "Los Angeles", "London"],
            "zhtw_display_name": ["紐約", "洛杉磯", "倫敦"],
            "english_address": ["NYC", "LA", "London"],
            "zhtw_address": ["紐約市", "洛杉磯市", "倫敦市"],
            "phone_number": ["+1234567890", "+1234567890", "+44123456789"],
            "rating": [4.5, 4.3, 4.8],
            "photo_urls": ["photo1", "photo2", "photo3"],
            "google_map_url": ["url1", "url2", "url3"],
            "website_url": ["web1", "web2", "web3"],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "primary_type": ["city", "city", "city"],
            "opening_hours": ["9am-5pm", "9am-5pm", "9am-5pm"],
            "country_code": ["US", "US", "GB"],
            "english_administrative_area": ["NY", "CA", "England"],
            "zhtw_administrative_area": ["紐約", "加州", "英格蘭"],
            "english_locality": ["Manhattan", "Downtown", "West End"],
            "zhtw_locality": ["曼哈頓", "市中心", "西區"],
            "report": ["report1", "report2", "report3"],
        }
    )


@pytest.fixture
def sample_property_df():
    """Sample property DataFrame from OLTP source."""
    return pd.DataFrame(
        {
            "id": [401, 402, 403],
            "slug": ["food", "culture", "nature"],
            "zhtw_display_name": ["食物", "文化", "自然"],
            "english_display_name": ["Food", "Culture", "Nature"],
            "category_type": ["category", "category", "category"],
            "zhtw_description": ["食物描述", "文化描述", "自然描述"],
            "source": ["manual", "manual", "manual"],
            "source_url": ["url1", "url2", "url3"],
            "is_active": [True, True, True],
            "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "emoji": ["🍕", "🎭", "🌲"],
            "english_description": ["Food desc", "Culture desc", "Nature desc"],
            "cover_img_url": ["cover1", "cover2", "cover3"],
        }
    )


# ============================================================================
# Tests for ID Mapping in Transform
# ============================================================================


class TestIDMappingPreservation:
    """Test that source IDs are properly preserved during transformation."""

    @patch("pipeline.transform.logger")
    def test_users_source_id_preserved(self, mock_logger, sample_users_df):
        """Source user IDs should be preserved in mapping."""
        from pipeline.transform import transform_users

        # Transform should rename 'id' to 'source_user_id'
        result = transform_users(sample_users_df)

        # Verify source IDs are preserved in the result
        assert "source_user_id" in result.columns
        assert result["source_user_id"].tolist() == [101, 102, 103]

    @patch("pipeline.transform.logger")
    def test_content_source_id_preserved(self, mock_logger, sample_content_df):
        """Source content IDs should be preserved in mapping."""
        from pipeline.transform import transform_content

        result = transform_content(sample_content_df)

        # Verify source IDs are preserved
        assert "source_content_id" in result.columns
        assert result["source_content_id"].tolist() == [201, 202, 203]

    @patch("pipeline.transform.logger")
    def test_places_source_id_preserved(self, mock_logger, sample_places_df):
        """Source place IDs should be preserved in mapping."""
        from pipeline.transform import transform_places

        result = transform_places(sample_places_df)

        # Verify source IDs are preserved
        assert "source_place_id" in result.columns
        assert result["source_place_id"].tolist() == [301, 302, 303]

    @patch("pipeline.transform.logger")
    def test_property_source_id_preserved(self, mock_logger, sample_property_df):
        """Source property IDs should be preserved in mapping."""
        from pipeline.transform import transform_property

        result = transform_property(sample_property_df)

        # Verify source IDs are preserved
        assert "source_property_id" in result.columns
        assert result["source_property_id"].tolist() == [401, 402, 403]


# ============================================================================
# Tests for ID Mapping in Fact Table
# ============================================================================


class TestFactTableIDMapping:
    """Test fact table creation with proper ID mapping."""

    @patch("pipeline.transform.logger")
    def test_fact_table_id_mapping(self, mock_logger):
        """Fact table should properly map OLTP IDs to OLAP IDs."""
        from pipeline.transform import transform_fact_table

        # Sample fact table data
        raw_fact = pd.DataFrame(
            {
                "user_id": [101, 102, 103],
                "content_id": [201, 202, 203],
                "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        )
        raw_content_places = pd.DataFrame(
            {
                "content_id": [201, 202, 203],
                "place_id": [301, 302, 303],
            }
        )
        raw_place_properties = pd.DataFrame(
            {
                "place_id": [301, 302, 303],
                "property_id": [401, 402, 403],
            }
        )

        # Create ID maps (simulating what transform orchestrator would create)
        user_id_map = {101: 1, 102: 2, 103: 3}  # OLTP ID -> OLAP ID
        content_id_map = {201: 1, 202: 2, 203: 3}
        place_id_map = {301: 1, 302: 2, 303: 3}
        property_id_map = {401: 1, 402: 2, 403: 3}

        result = transform_fact_table(
            raw_fact,
            raw_content_places,
            raw_place_properties,
            user_id_map=user_id_map,
            content_id_map=content_id_map,
            place_id_map=place_id_map,
            property_id_map=property_id_map,
        )

        # Verify IDs are mapped correctly
        assert result["user_id"].tolist() == [1, 2, 3]
        assert result["content_id"].tolist() == [1, 2, 3]

    @patch("pipeline.transform.logger")
    def test_fact_table_unmapped_ids_dropped(self, mock_logger):
        """Rows with unmapped IDs should be dropped."""
        from pipeline.transform import transform_fact_table

        raw_fact = pd.DataFrame(
            {
                "user_id": [101, 102, 999],  # 999 not in map
                "content_id": [201, 202, 203],
                "created_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        )
        raw_content_places = pd.DataFrame(
            {
                "content_id": [201, 202, 203],
                "place_id": [301, 302, 303],
            }
        )
        raw_place_properties = pd.DataFrame(
            {
                "place_id": [301, 302, 303],
                "property_id": [401, 402, 403],
            }
        )

        user_id_map = {101: 1, 102: 2}  # 999 not mapped
        content_id_map = {201: 1, 202: 2, 203: 3}
        place_id_map = {301: 1, 302: 2, 303: 3}
        property_id_map = {401: 1, 402: 2, 403: 3}

        result = transform_fact_table(
            raw_fact,
            raw_content_places,
            raw_place_properties,
            user_id_map=user_id_map,
            content_id_map=content_id_map,
            place_id_map=place_id_map,
            property_id_map=property_id_map,
        )

        # Should only have 2 rows (third row with unmapped ID dropped)
        assert len(result) == 2
        assert result["user_id"].tolist() == [1, 2]


# ============================================================================
# Tests for Field Structure Validation
# ============================================================================


class TestFieldStructureValidation:
    """Test that invalid field structures are caught with proper logging."""

    @patch("pipeline.transform.logger")
    def test_missing_required_column_users(self, mock_logger):
        """Missing required column in users should raise ValueError."""
        from pipeline.transform import transform_users

        # Missing 'email' column
        invalid_df = pd.DataFrame(
            {
                "id": [101, 102],
                "display_name": ["Alice", "Bob"],
                # Missing 'email' and other required fields
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_users(invalid_df)

    @patch("pipeline.transform.logger")
    def test_missing_required_column_content(self, mock_logger):
        """Missing required column in content should raise ValueError."""
        from pipeline.transform import transform_content

        invalid_df = pd.DataFrame(
            {
                "id": [201, 202],
                # Missing required columns
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_content(invalid_df)

    @patch("pipeline.transform.logger")
    def test_missing_required_column_places(self, mock_logger):
        """Missing required column in places should raise ValueError."""
        from pipeline.transform import transform_places

        invalid_df = pd.DataFrame(
            {
                "id": [301, 302],
                # Missing required columns
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_places(invalid_df)

    @patch("pipeline.transform.logger")
    def test_missing_required_column_property(self, mock_logger):
        """Missing required column in property should raise ValueError."""
        from pipeline.transform import transform_property

        invalid_df = pd.DataFrame(
            {
                "id": [401, 402],
                # Missing required columns
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_property(invalid_df)


# ============================================================================
# Tests for Load-Phase Field Validation
# ============================================================================


class TestLoadPhaseValidation:
    """Test field validation and error logging during load phase."""

    @patch("pipeline.load.psycopg2.connect")
    @patch("pipeline.load.logger")
    def test_load_handles_missing_columns(self, mock_logger, mock_connect):
        """Load phase should log which table fails on missing columns."""
        from pipeline.load import load_table

        # DataFrame missing a column that OLAP schema expects
        invalid_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                # Missing expected columns for OLAP
            }
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate a column mismatch error during insert

        mock_cursor.execute.side_effect = Exception("Column type mismatch")

        # Should attempt to load but fail with appropriate error
        with pytest.raises(ValueError, match="Failed to load table 'users'"):
            load_table("users", invalid_df, {})

    @patch("pipeline.load.psycopg2.connect")
    @patch("pipeline.load.logger")
    def test_load_logs_table_name_on_error(self, mock_logger, mock_connect):
        """Load should log the table name when an error occurs."""
        from pipeline.load import load_table

        valid_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate cursor execute failure
        mock_cursor.execute.side_effect = Exception("Column type mismatch")

        with pytest.raises(ValueError, match="Failed to load table 'users'"):
            load_table("users", valid_df, {})

    @patch("pipeline.load.psycopg2.connect")
    @patch("pipeline.load.logger")
    def test_load_empty_dataframe_logs_warning(self, mock_logger, mock_connect):
        """Load should log warning for empty DataFrame and skip loading."""
        from pipeline.load import load_table

        empty_df = pd.DataFrame()

        load_table("users", empty_df, {})

        # Should call logger.warning with table name
        mock_logger.warning.assert_called()
        call_args = str(mock_logger.warning.call_args)
        assert "users" in call_args

    @patch("pipeline.load.get_olap_connection_params")
    @patch("pipeline.load.psycopg2.connect")
    @patch("pipeline.load.logger")
    def test_load_orchestrator_validates_all_tables(
        self, mock_logger, mock_connect, mock_get_params
    ):
        """Load orchestrator should validate that all required tables are present."""
        from pipeline.load import load_olap

        mock_get_params.return_value = {}

        # Missing required tables
        incomplete_data = {
            "users": pd.DataFrame({"id": [1]}),
            # Missing content, places, property, fact_table
        }

        with pytest.raises(
            ValueError, match="OLAP load failed.*Missing required tables"
        ):
            load_olap(incomplete_data)


# ============================================================================
# Tests for Type Consistency
# ============================================================================


class TestIDTypeConsistency:
    """Test that source and target ID types are consistent (integers)."""

    def test_source_ids_are_integers(self, sample_users_df, sample_content_df):
        """Source IDs should be integers."""
        assert sample_users_df["id"].dtype in ["int64", "int32", int]
        assert sample_content_df["id"].dtype in ["int64", "int32", int]

    @patch("pipeline.transform.logger")
    def test_olap_ids_remain_integers(self, mock_logger, sample_users_df):
        """OLAP IDs should remain integers after transformation."""
        from pipeline.transform import transform_users

        result = transform_users(sample_users_df)

        # source_user_id should be integer type
        assert result["source_user_id"].dtype in ["int64", "int32", int]

    def test_id_mapping_type_consistency(self):
        """ID mappings should preserve integer types."""
        # Source IDs (integers)
        source_ids = [101, 102, 103]
        # Mapped IDs (integers)
        mapped_ids = [1, 2, 3]

        id_map = dict(zip(source_ids, mapped_ids))

        # All keys and values should be integers
        assert all(isinstance(k, int) for k in id_map.keys())
        assert all(isinstance(v, int) for v in id_map.values())
