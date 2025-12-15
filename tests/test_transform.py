"""
Tests for the transformation module.

Tests each transformation function with sample data to ensure correct
schema mapping from OLTP to OLAP.
"""

import pandas as pd
import pytest
from datetime import datetime

from pipeline.transform import (
    transform_users,
    transform_content,
    transform_places,
    transform_property,
    transform_fact_table,
    transform,
)


# Sample test data generators
def create_sample_users(n: int = 5) -> pd.DataFrame:
    """Create sample OLTP users data."""
    return pd.DataFrame(
        {
            "id": [f"user-{i}" for i in range(n)],
            "email": [f"user{i}@example.com" for i in range(n)],
            "display_name": [f"User {i}" for i in range(n)],
            "first_name": [f"First{i}" for i in range(n)],
            "last_name": [f"Last{i}" for i in range(n)],
            "avatar_url": [f"https://example.com/avatar{i}.png" for i in range(n)],
            "gender": ["male", "female", "other", "male", "female"][:n],
            "provider": ["google", "github", "email", "google", "github"][:n],
            "found_us_source": [
                "instagram",
                "linkedin",
                "threads",
                "friends",
                "web_summit",
            ][:n],
            "created_at": [datetime(2024, 1, 1) for _ in range(n)],
            "updated_at": [datetime(2024, 1, 2) for _ in range(n)],
        }
    )


def create_sample_content(n: int = 5) -> pd.DataFrame:
    """Create sample OLTP content data."""
    return pd.DataFrame(
        {
            "id": [f"content-{i}" for i in range(n)],
            "url": [f"https://example.com/content{i}" for i in range(n)],
            "html": [f"<html>Content {i}</html>" for i in range(n)],
            "title": [f"Content {i}" for i in range(n)],
            "description": [f"Content {i} description" for i in range(n)],
            "site_name": ["Example Site" for _ in range(n)],
            "icon_url": [f"https://example.com/icon{i}.png" for i in range(n)],
            "preview_image_url": [
                f"https://example.com/preview{i}.jpg" for i in range(n)
            ],
            "status": ["published", "draft", "published", "draft", "published"][:n],
            "created_at": [datetime(2024, 1, 1) for _ in range(n)],
            "updated_at": [datetime(2024, 1, 2) for _ in range(n)],
        }
    )


def create_sample_places(n: int = 5) -> pd.DataFrame:
    """Create sample OLTP places data."""
    return pd.DataFrame(
        {
            "id": [f"place-{i}" for i in range(n)],
            "google_maps_id": [f"gmaps-{i}" for i in range(n)],
            "english_display_name": [f"Place {i}" for i in range(n)],
            "zhtw_display_name": [f"地點 {i}" for i in range(n)],
            "english_address": [f"{i} Main St" for i in range(n)],
            "zhtw_address": [f"{i} 主街" for i in range(n)],
            "phone_number": [f"+1-555-000{i}" for i in range(n)],
            "rating": [4.0 + i * 0.2 for i in range(n)],
            "latitude": [40.0 + i * 0.1 for i in range(n)],
            "longitude": [-74.0 + i * 0.1 for i in range(n)],
            "photo_urls": [[f"https://example.com/photo{i}.jpg"] for i in range(n)],
            "google_map_url": [f"https://maps.google.com/place{i}" for i in range(n)],
            "website_url": [f"https://example.com/place{i}" for i in range(n)],
            "primary_type": ["restaurant", "cafe", "bar", "restaurant", "cafe"][:n],
            "opening_hours": ["9AM-5PM" for _ in range(n)],
            "country_code": ["US", "US", "TW", "TW", "JP"][:n],
            "english_administrative_area": ["NY", "NY", "Taipei", "Taipei", "Tokyo"][
                :n
            ],
            "zhtw_administrative_area": ["紐約", "紐約", "台北", "台北", "東京"][:n],
            "english_locality": ["NYC", "NYC", "Taipei", "Taipei", "Tokyo"][:n],
            "zhtw_locality": ["紐約市", "紐約市", "台北市", "台北市", "東京都"][:n],
            "report": [None for _ in range(n)],
            "created_at": [datetime(2024, 1, 1) for _ in range(n)],
            "updated_at": [datetime(2024, 1, 2) for _ in range(n)],
        }
    )


def create_sample_property_mapping(n: int = 5) -> pd.DataFrame:
    """Create sample OLTP property_mapping data."""
    return pd.DataFrame(
        {
            "id": [f"property-{i}" for i in range(n)],
            "slug": [f"property-{i}" for i in range(n)],
            "english_display_name": [f"Property {i}" for i in range(n)],
            "zhtw_display_name": [f"屬性 {i}" for i in range(n)],
            "english_description": [f"Property {i} description" for i in range(n)],
            "zhtw_description": [f"屬性 {i} 描述" for i in range(n)],
            "category_type": ["label", "ranking", "award", "campaign", "label"][:n],
            "source": ["internal", "external", "internal", "external", "internal"][:n],
            "source_url": [f"https://example.com/prop{i}" for i in range(n)],
            "is_active": [True, True, False, True, True][:n],
            "emoji": ["🏆", "⭐", "🎖️", "🔥", "✨"][:n],
            "cover_img_url": [f"https://example.com/cover{i}.jpg" for i in range(n)],
            "created_at": [datetime(2024, 1, 1) for _ in range(n)],
            "updated_at": [datetime(2024, 1, 2) for _ in range(n)],
        }
    )


def create_sample_user_contents(user_ids: list, content_ids: list) -> pd.DataFrame:
    """Create sample OLTP user_contents junction table data."""
    data = []
    for user_id in user_ids:
        for content_id in content_ids[:3]:  # Each user has 3 content items
            data.append(
                {
                    "user_id": user_id,
                    "content_id": content_id,
                    "is_deleted": False,
                    "created_at": datetime(2024, 1, 1),
                    "updated_at": datetime(2024, 1, 2),
                }
            )
    return pd.DataFrame(data)


# Test transform_users
class TestTransformUsers:
    """Tests for transform_users function."""

    def test_basic_transformation(self):
        """Test basic users transformation."""
        raw_users = create_sample_users()
        result = transform_users(raw_users)

        assert "id" in result.columns
        assert "source_user_id" in result.columns
        assert len(result) == 5
        assert result["id"].tolist() == [1, 2, 3, 4, 5]
        assert result["source_user_id"].tolist() == [f"user-{i}" for i in range(5)]

    def test_column_selection(self):
        """Test that only required columns are selected."""
        raw_users = create_sample_users()
        result = transform_users(raw_users)

        expected_columns = {
            "id",
            "source_user_id",
            "email",
            "display_name",
            "avatar_url",
            "found_us_source",
            "created_at",
            "updated_at",
        }
        assert set(result.columns) == expected_columns

    def test_duplicate_removal(self):
        """Test that duplicates are removed."""
        raw_users = create_sample_users(3)
        # Add a duplicate row
        duplicate = raw_users.iloc[0].copy()
        raw_users = pd.concat([raw_users, pd.DataFrame([duplicate])], ignore_index=True)

        result = transform_users(raw_users)

        assert len(result) == 3  # Duplicate removed

    def test_missing_columns_error(self):
        """Test error handling for missing columns."""
        raw_users = create_sample_users()
        raw_users = raw_users.drop(columns=["email"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_users(raw_users)


# Test transform_content
class TestTransformContent:
    """Tests for transform_content function."""

    def test_basic_transformation(self):
        """Test basic content transformation."""
        raw_content = create_sample_content()
        result = transform_content(raw_content)

        assert "id" in result.columns
        assert "source_content_id" in result.columns
        assert len(result) == 5
        assert result["id"].tolist() == [1, 2, 3, 4, 5]

    def test_column_selection(self):
        """Test that only required columns are selected."""
        raw_content = create_sample_content()
        result = transform_content(raw_content)

        expected_columns = {
            "id",
            "source_content_id",
            "platform",
            "platform_id",
            "url",
            "thumbnail_url",
            "description",
            "created_at",
            "updated_at",
        }
        assert set(result.columns) == expected_columns

    def test_duplicate_removal(self):
        """Test that duplicates are removed."""
        raw_content = create_sample_content(3)
        # Add a duplicate row
        duplicate = raw_content.iloc[0].copy()
        raw_content = pd.concat(
            [raw_content, pd.DataFrame([duplicate])], ignore_index=True
        )

        result = transform_content(raw_content)

        assert len(result) == 3  # Duplicate removed

    def test_missing_columns_error(self):
        """Test error handling for missing columns."""
        raw_content = create_sample_content()
        raw_content = raw_content.drop(columns=["status"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_content(raw_content)


# Test transform_places
class TestTransformPlaces:
    """Tests for transform_places function."""

    def test_basic_transformation(self):
        """Test basic places transformation."""
        raw_places = create_sample_places()
        result = transform_places(raw_places)

        assert "id" in result.columns
        assert "source_place_id" in result.columns
        assert len(result) == 5
        assert result["id"].tolist() == [1, 2, 3, 4, 5]

    def test_column_selection(self):
        """Test that only required columns are selected."""
        raw_places = create_sample_places()
        result = transform_places(raw_places)

        expected_columns = {
            "id",
            "source_place_id",
            "google_maps_id",
            "english_display_name",
            "zhtw_display_name",
            "english_address",
            "zhtw_address",
            "phone_number",
            "rating",
            "latitude",
            "longitude",
            "country_code",
            "english_administrative_area",
            "zhtw_administrative_area",
            "english_locality",
            "zhtw_locality",
            "primary_type",
            "created_at",
            "updated_at",
        }
        assert set(result.columns) == expected_columns

    def test_duplicate_removal(self):
        """Test that duplicates are removed."""
        raw_places = create_sample_places(3)
        # Add a duplicate row
        duplicate = raw_places.iloc[0].copy()
        raw_places = pd.concat(
            [raw_places, pd.DataFrame([duplicate])], ignore_index=True
        )

        result = transform_places(raw_places)

        assert len(result) == 3  # Duplicate removed

    def test_missing_columns_error(self):
        """Test error handling for missing columns."""
        raw_places = create_sample_places()
        raw_places = raw_places.drop(columns=["google_maps_id"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_places(raw_places)


# Test transform_property
class TestTransformProperty:
    """Tests for transform_property function."""

    def test_basic_transformation(self):
        """Test basic property transformation."""
        raw_property = create_sample_property_mapping()
        result = transform_property(raw_property)

        assert "id" in result.columns
        assert "source_property_id" in result.columns
        assert len(result) == 5
        assert result["id"].tolist() == [1, 2, 3, 4, 5]

    def test_column_selection(self):
        """Test that only required columns are selected."""
        raw_property = create_sample_property_mapping()
        result = transform_property(raw_property)

        expected_columns = {
            "id",
            "source_property_id",
            "english_name",
            "zhtw_name",
            "emoji",
            "category_type",
            "created_at",
            "updated_at",
        }
        assert set(result.columns) == expected_columns

    def test_duplicate_removal(self):
        """Test that duplicates are removed."""
        raw_property = create_sample_property_mapping(3)
        # Add a duplicate row
        duplicate = raw_property.iloc[0].copy()
        raw_property = pd.concat(
            [raw_property, pd.DataFrame([duplicate])], ignore_index=True
        )

        result = transform_property(raw_property)

        assert len(result) == 3  # Duplicate removed

    def test_missing_columns_error(self):
        """Test error handling for missing columns."""
        raw_property = create_sample_property_mapping()
        raw_property = raw_property.drop(columns=["category_type"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_property(raw_property)


# Test transform_fact_table
class TestTransformFactTable:
    """Tests for transform_fact_table function."""

    def test_basic_transformation(self):
        """Test basic fact table transformation."""
        user_ids = [f"user-{i}" for i in range(3)]
        content_ids = [f"content-{i}" for i in range(3)]

        raw_user_contents = create_sample_user_contents(user_ids, content_ids)

        user_id_map = {uid: uid for uid in user_ids}
        content_id_map = {cid: cid for cid in content_ids}
        place_id_map = {}
        property_id_map = {}

        result = transform_fact_table(
            raw_user_contents,
            user_id_map=user_id_map,
            content_id_map=content_id_map,
            place_id_map=place_id_map,
            property_id_map=property_id_map,
        )

        assert "user_id" in result.columns
        assert "content_id" in result.columns
        assert "place_id" in result.columns
        assert "property_id" in result.columns
        assert len(result) == 9  # 3 users * 3 content items each

    def test_id_mapping(self):
        """Test that ID mapping is applied correctly."""
        user_ids = ["user-1", "user-2"]
        content_ids = ["content-1", "content-2"]

        raw_user_contents = create_sample_user_contents(user_ids, content_ids)

        user_id_map = {"user-1": "mapped-user-1", "user-2": "mapped-user-2"}
        content_id_map = {
            "content-1": "mapped-content-1",
            "content-2": "mapped-content-2",
        }

        result = transform_fact_table(
            raw_user_contents,
            user_id_map=user_id_map,
            content_id_map=content_id_map,
            place_id_map={},
            property_id_map={},
        )

        # Check that IDs are mapped
        assert "mapped-user-1" in result["user_id"].values
        assert "mapped-content-1" in result["content_id"].values

    def test_missing_columns_error(self):
        """Test error handling for missing columns."""
        raw_user_contents = create_sample_user_contents(["user-1"], ["content-1"])
        raw_user_contents = raw_user_contents.drop(columns=["user_id"])

        with pytest.raises(ValueError, match="Missing required columns"):
            transform_fact_table(
                raw_user_contents,
                user_id_map={},
                content_id_map={},
                place_id_map={},
                property_id_map={},
            )


# Test main transform orchestrator
class TestTransformOrchestrator:
    """Tests for the main transform function."""

    def test_complete_transformation(self):
        """Test complete transformation of all tables."""
        raw_data = {
            "users": create_sample_users(3),
            "contents": create_sample_content(3),
            "places": create_sample_places(3),
            "property_mapping": create_sample_property_mapping(3),
            "user_contents": create_sample_user_contents(
                [f"user-{i}" for i in range(3)], [f"content-{i}" for i in range(3)]
            ),
        }

        result = transform(raw_data)

        # Check that all tables are present
        assert "users" in result
        assert "content" in result
        assert "places" in result
        assert "property" in result
        assert "fact_table" in result

        # Check that all are DataFrames
        for table in result.values():
            assert isinstance(table, pd.DataFrame)

        # Check dimensions
        assert len(result["users"]) == 3
        assert len(result["content"]) == 3
        assert len(result["places"]) == 3
        assert len(result["property"]) == 3
        assert len(result["fact_table"]) == 9  # 3 users * 3 content items

    def test_missing_tables_error(self):
        """Test error handling for missing tables."""
        raw_data = {
            "users": create_sample_users(),
            "contents": create_sample_content(),
            # Missing places, property_mapping, and user_contents
        }

        with pytest.raises(ValueError, match="Missing required tables"):
            transform(raw_data)

    def test_transformation_with_empty_data(self):
        """Test transformation with empty DataFrames."""
        raw_data = {
            "users": pd.DataFrame(
                columns=[
                    "id",
                    "email",
                    "display_name",
                    "first_name",
                    "last_name",
                    "avatar_url",
                    "gender",
                    "provider",
                    "found_us_source",
                    "created_at",
                    "updated_at",
                ]
            ),
            "contents": pd.DataFrame(
                columns=[
                    "id",
                    "url",
                    "html",
                    "title",
                    "description",
                    "site_name",
                    "icon_url",
                    "preview_image_url",
                    "status",
                    "created_at",
                    "updated_at",
                ]
            ),
            "places": create_sample_places(0),
            "property_mapping": create_sample_property_mapping(0),
            "user_contents": pd.DataFrame(
                columns=[
                    "user_id",
                    "content_id",
                    "status",
                    "is_deleted",
                    "created_at",
                    "updated_at",
                ]
            ),
        }

        result = transform(raw_data)

        assert len(result["users"]) == 0
        assert len(result["content"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
