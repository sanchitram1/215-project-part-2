-- OLAP Star Schema for Voyla Analytics
-- Dimension tables and fact table for user behavior analysis

-- 1. Users Dimension Table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    source_user_id INTEGER UNIQUE NOT NULL,
    email TEXT,
    display_name TEXT,
    avatar_url TEXT,
    found_us_source TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_users_source ON users (found_us_source);
CREATE INDEX idx_users_created ON users (created_at);

-- 2. Places Dimension Table
CREATE TABLE places (
    id SERIAL PRIMARY KEY,
    source_place_id INTEGER UNIQUE NOT NULL,
    google_maps_id TEXT,
    english_display_name TEXT,
    zhtw_display_name TEXT,
    english_address TEXT,
    zhtw_address TEXT,
    phone_number TEXT,
    rating DECIMAL(2, 1),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    country_code VARCHAR(2),
    english_administrative_area TEXT,
    zhtw_administrative_area TEXT,
    english_locality TEXT,
    zhtw_locality TEXT,
    primary_type TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_places_country ON places (country_code);
CREATE INDEX idx_places_locality ON places (english_locality);
CREATE INDEX idx_places_rating ON places (rating);
CREATE INDEX idx_places_coords ON places (latitude, longitude);

-- 3. Content Dimension Table
CREATE TABLE content (
    id SERIAL PRIMARY KEY,
    source_content_id INTEGER UNIQUE NOT NULL,
    platform TEXT,
    platform_id TEXT,
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
    id SERIAL PRIMARY KEY,
    source_property_id INTEGER UNIQUE NOT NULL,
    english_name TEXT,
    zhtw_name TEXT,
    emoji TEXT,
    category_type TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_property_category ON property (category_type);

-- 5. interactions Fact Table
CREATE TABLE interactions (
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    content_id INTEGER NOT NULL REFERENCES content (id) ON DELETE CASCADE,
    place_id INTEGER NOT NULL REFERENCES places (id) ON DELETE CASCADE,
    property_id INTEGER REFERENCES property (id) ON DELETE SET NULL,
    source_user_id INTEGER,
    source_content_id INTEGER,
    source_place_id INTEGER,
    source_property_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, content_id, place_id, property_id)
);

CREATE INDEX idx_interactions_user ON interactions (user_id);
CREATE INDEX idx_interactions_content ON interactions (content_id);
CREATE INDEX idx_interactions_place ON interactions (place_id);
CREATE INDEX idx_interactions_property ON interactions (property_id);
CREATE INDEX idx_interactions_created ON interactions (created_at);
