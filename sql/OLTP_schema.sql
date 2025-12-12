-- 1. Users Table
create table users (
    id uuid primary key default gen_random_uuid(),
    email varchar(255)
    unique not null
    check (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'),
    display_name varchar(100),
    avatar_url text,
    found_us_source varchar(50)
    not null
    check (
        found_us_source in (
            'instagram',
            'linkedin',
            'threads',
            'friends',
            'web_summit',
            'app_store',
            'organic',
            'other'
        )
    ),
    created_at timestamp default now(),
    updated_at timestamp default now()
);

create index idx_users_source on users (found_us_source);
create index idx_users_created on users (created_at);

-- 2. Places Table
create table places (
    id uuid primary key default gen_random_uuid(),
    google_maps_id varchar(200) unique not null,
    english_display_name varchar(255) not null,
    zhtw_display_name varchar(255),
    english_address text,
    zhtw_address text,
    phone_number varchar(50),
    rating decimal(2, 1) check (rating >= 0 and rating <= 5),
    latitude decimal(10, 8) check (latitude >= -90 and latitude <= 90),
    longitude decimal(11, 8) check (longitude >= -180 and longitude <= 180),
    country_code varchar(2),
    english_administrative_area varchar(255),
    zhtw_administrative_area varchar(255),
    english_locality varchar(255),
    zhtw_locality varchar(255),
    primary_type varchar(100),
    created_at timestamp default now(),
    updated_at timestamp default now()
);

create index idx_places_country on places (country_code);
create index idx_places_locality on places (english_locality);
create index idx_places_rating on places (rating);
create index idx_places_coords on places (latitude, longitude);

-- 3. Contents Table
create table contents (
    id uuid primary key default gen_random_uuid(),
    platform varchar(50)
    not null
    check (platform in ('instagram', 'tiktok', 'youtube')),
    platform_id varchar(255) not null,
    url text not null,
    thumbnail_url text,
    description text,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    unique (platform, platform_id)
);

create index idx_contents_platform on contents (platform);

-- 4. Tags Table
create table tags (
    id uuid primary key default gen_random_uuid(),
    english_name varchar(100) not null,
    zhtw_name varchar(100),
    emoji varchar(10),
    category varchar(100),
    created_at timestamp default now(),
    updated_at timestamp default now()
);

create index idx_tags_category on tags (category);

-- 5. Property_Mapping Table
create table property_mapping (
    id uuid primary key default gen_random_uuid(),
    english_name varchar(100) not null,
    zhtw_name varchar(100),
    emoji varchar(10) check (length(emoji) >= 1 and length(emoji) <= 8),
    category_type varchar(50)
    check (category_type in ('label', 'ranking', 'award', 'campaign')),
    created_at timestamp default now(),
    updated_at timestamp default now()
);

create index idx_property_category on property_mapping (category_type);

-- 6. User_Places Junction Table
create table user_places (
    user_id uuid not null references users (id) on delete cascade,
    place_id uuid not null references places (id) on delete cascade,
    is_deleted boolean default false,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    primary key (user_id, place_id)
);

create index idx_user_places_user on user_places (user_id);
create index idx_user_places_place on user_places (place_id);
create index idx_user_places_created on user_places (created_at);
create index idx_user_places_active on user_places (user_id)
where is_deleted = false;

-- 7. User_Contents Junction Table
create table user_contents (
    user_id uuid not null references users (id) on delete cascade,
    content_id uuid not null references contents (id) on delete cascade,
    is_deleted boolean default false,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    primary key (user_id, content_id)
);

create index idx_user_contents_user on user_contents (user_id);
create index idx_user_contents_content on user_contents (content_id);

-- 8. Content_Places Junction Table
create table content_places (
    content_id uuid not null references contents (id) on delete cascade,
    place_id uuid not null references places (id) on delete cascade,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    primary key (content_id, place_id)
);

create index idx_content_places_content on content_places (content_id);
create index idx_content_places_place on content_places (place_id);

-- 9. Place_Tags Junction Table
create table place_tags (
    place_id uuid not null references places (id) on delete cascade,
    tag_id uuid not null references tags (id) on delete cascade,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    primary key (place_id, tag_id)
);

create index idx_place_tags_place on place_tags (place_id);
create index idx_place_tags_tag on place_tags (tag_id);

-- 10. Place_Properties Junction Table
create table place_properties (
    place_id uuid not null references places (id) on delete cascade,
    property_id uuid not null references property_mapping (
        id
    ) on delete cascade,
    rank integer check (rank > 0),
    start_at date,
    end_at date,
    created_at timestamp default now(),
    updated_at timestamp default now(),
    primary key (place_id, property_id),
    check (end_at is null or start_at is null or end_at >= start_at)
);

create index idx_place_properties_place on place_properties (place_id);
create index idx_place_properties_property on place_properties (property_id);

-- 11. App_Version Table
create table app_version (
    id uuid primary key default gen_random_uuid(),
    version varchar(20) not null,
    min_version varchar(20) not null,
    force_update boolean default false,
    updated_at timestamp default now()
);
