-- OLAP Star Schema for Voyla Analytics
-- Dimension tables and fact table for user behavior analysis

-- 1. Users Dimension Table
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    display_name VARCHAR(100),
    avatar_url TEXT,
    found_us_source VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_users_source ON users (found_us_source);
CREATE INDEX idx_users_created ON users (created_at);

-- 2. Places Dimension Table
CREATE TABLE places (
    place_id UUID PRIMARY KEY,
    google_maps_id VARCHAR(200),
    english_display_name VARCHAR(255),
    zhtw_display_name VARCHAR(255),
    english_address TEXT,
    zhtw_address TEXT,
    phone_number VARCHAR(50),
    rating DECIMAL(2, 1),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    country_code VARCHAR(2),
    english_administrative_area VARCHAR(255),
    zhtw_administrative_area VARCHAR(255),
    english_locality VARCHAR(255),
    zhtw_locality VARCHAR(255),
    primary_type VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_places_country ON places (country_code);
CREATE INDEX idx_places_locality ON places (english_locality);
CREATE INDEX idx_places_rating ON places (rating);
CREATE INDEX idx_places_coords ON places (latitude, longitude);

-- 3. Content Dimension Table
CREATE TABLE content (
    content_id UUID PRIMARY KEY,
    platform VARCHAR(50),
    platform_id VARCHAR(255),
    url TEXT,
    thumbnail_url TEXT,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_content_platform ON content (platform);
CREATE INDEX idx_content_created ON content (created_at);

-- 4. Property Dimension Table
CREATE TABLE property (
    property_id UUID PRIMARY KEY,
    english_name VARCHAR(100),
    zhtw_name VARCHAR(100),
    emoji VARCHAR(10),
    category_type VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_property_category ON property (category_type);

-- 5. interactions Fact Table
CREATE TABLE interactions (
    user_id UUID NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    content_id UUID NOT NULL REFERENCES content (content_id) ON DELETE CASCADE,
    place_id UUID NOT NULL REFERENCES places (place_id) ON DELETE CASCADE,
    property_id UUID REFERENCES property (property_id) ON DELETE SET NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, content_id, place_id, property_id)
);

CREATE INDEX idx_interactions_user ON interactions (user_id);
CREATE INDEX idx_interactions_content ON interactions (content_id);
CREATE INDEX idx_interactions_place ON interactions (place_id);
CREATE INDEX idx_interactions_property ON interactions (property_id);
CREATE INDEX idx_interactions_created ON interactions (created_at);
