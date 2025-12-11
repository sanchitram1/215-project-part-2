DROP TABLE IF EXISTS interactions CASCADE;
DROP TABLE IF EXISTS property    CASCADE;
DROP TABLE IF EXISTS content     CASCADE;
DROP TABLE IF EXISTS places      CASCADE;
DROP TABLE IF EXISTS users       CASCADE;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL
                    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'),
    display_name    VARCHAR(100),
    avatar_url      TEXT,
    found_us_source VARCHAR(50) NOT NULL
                    CHECK (found_us_source IN (
                        'instagram',
                        'tiktok',
                        'youtube',
                        'threads',
                        'friends',
                        'web_summit',
                        'app_store',
                        'organic',
                        'other'
                    )),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_users_found_source ON users(found_us_source);
CREATE INDEX idx_users_created_at   ON users(created_at);

-- DIMENSION: PLACES

CREATE TABLE places (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    google_maps_id              VARCHAR(200) UNIQUE NOT NULL,
    english_display_name        VARCHAR(255) NOT NULL,
    zhtw_display_name           VARCHAR(255),
    english_address             TEXT,
    zhtw_address                TEXT,
    phone_number                VARCHAR(50),
    rating                      DECIMAL(2,1)
                                CHECK (rating BETWEEN 0 AND 5),
    latitude                    DECIMAL(10,8)
                                CHECK (latitude BETWEEN -90 AND 90),
    longitude                   DECIMAL(11,8)
                                CHECK (longitude BETWEEN -180 AND 180),
    country_code                VARCHAR(2),
    english_administrative_area VARCHAR(255),
    zhtw_administrative_area    VARCHAR(255),
    english_locality            VARCHAR(255),
    zhtw_locality               VARCHAR(255),
    primary_type                VARCHAR(100),
    created_at                  TIMESTAMP DEFAULT NOW(),
    updated_at                  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_places_country   ON places(country_code);
CREATE INDEX idx_places_locality  ON places(english_locality);
CREATE INDEX idx_places_rating    ON places(rating);
CREATE INDEX idx_places_lat_lng   ON places(latitude, longitude);

-- DIMENSION: CONTENT

CREATE TABLE content (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform       VARCHAR(50) NOT NULL
                   CHECK (platform IN ('instagram', 'tiktok', 'youtube')),
    platform_id    VARCHAR(255) NOT NULL,
    url            TEXT NOT NULL,
    thumbnail_url  TEXT,
    description    TEXT,

    -- data from social media
    upload_time    TIMESTAMP,
    like_count     INTEGER CHECK (like_count >= 0),
    comment_count  INTEGER CHECK (comment_count >= 0),

    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW(),

    UNIQUE (platform, platform_id)
);

CREATE INDEX idx_content_platform    ON content(platform);
CREATE INDEX idx_content_upload_time ON content(upload_time);

-- DIMENSION: PROPERTY

CREATE TABLE property (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    english_name  VARCHAR(100) NOT NULL,
    zhtw_name     VARCHAR(100),
    emoji         VARCHAR(10)
                  CHECK (LENGTH(emoji) BETWEEN 1 AND 8),
    category_type VARCHAR(50)
                  CHECK (category_type IN ('label', 'ranking', 'award', 'campaign')),
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_property_category ON property(category_type);

-- FACT TABLE: INTERACTIONS

CREATE TABLE interactions (
    user_id      UUID NOT NULL REFERENCES users(id),
    content_id   UUID NOT NULL REFERENCES content(id),
    place_id     UUID NOT NULL REFERENCES places(id),
    property_id  UUID NOT NULL REFERENCES property(id),


    interaction_count     INTEGER NOT NULL DEFAULT 1
                          CHECK (interaction_count > 0),
    first_interaction_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    last_interaction_at   TIMESTAMP NOT NULL DEFAULT NOW(),


    PRIMARY KEY (user_id, content_id, place_id, property_id)
);

CREATE INDEX idx_interactions_user      ON interactions(user_id);
CREATE INDEX idx_interactions_place     ON interactions(place_id);
CREATE INDEX idx_interactions_content   ON interactions(content_id);
CREATE INDEX idx_interactions_property  ON interactions(property_id);
CREATE INDEX idx_interactions_last_time ON interactions(last_interaction_at);
