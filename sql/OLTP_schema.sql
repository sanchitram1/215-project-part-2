-- public.contents definition

-- Drop table

-- DROP TABLE public.contents;

CREATE TABLE public.contents (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	url text NOT NULL,
	html text NULL,
	title text NULL,
	description text NULL,
	site_name text NULL,
	icon_url text NULL,
	preview_image_url text NULL,
	status text DEFAULT 'waiting'::text NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT contents_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_contents_created_at ON public.contents USING btree (created_at);
CREATE INDEX idx_contents_site_name ON public.contents USING btree (site_name);


-- public.places definition

-- Drop table

-- DROP TABLE public.places;

CREATE TABLE public.places (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	google_maps_id text NOT NULL,
	latitude numeric NULL,
	longitude numeric NULL,
	english_display_name text NULL,
	zhtw_display_name text NULL,
	english_address text NULL,
	zhtw_address text NULL,
	phone_number text NULL,
	rating numeric NULL,
	photo_urls _text DEFAULT '{}'::text[] NULL,
	google_map_url text NULL,
	website_url text NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	primary_type text NULL,
	opening_hours _text NULL,
	country_code text NULL,
	english_administrative_area text NULL,
	zhtw_administrative_area text NULL,
	english_locality text NULL,
	zhtw_locality text NULL,
	report bool NULL,
	CONSTRAINT check_valid_coordinates CHECK ((((latitude IS NULL) AND (longitude IS NULL)) OR (((latitude >= ('-90'::integer)::numeric) AND (latitude <= (90)::numeric)) AND ((longitude >= ('-180'::integer)::numeric) AND (longitude <= (180)::numeric))))),
	CONSTRAINT check_valid_rating CHECK (((rating IS NULL) OR ((rating >= (0)::numeric) AND (rating <= (5)::numeric)))),
	CONSTRAINT places_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_places_country_code ON public.places USING btree (country_code);
CREATE INDEX idx_places_created_at ON public.places USING btree (created_at);
CREATE INDEX idx_places_google_maps_id ON public.places USING btree (google_maps_id);
CREATE INDEX idx_places_locality ON public.places USING btree (english_locality);


-- public.property_mapping definition

-- Drop table

-- DROP TABLE public.property_mapping;

CREATE TABLE public.property_mapping (
	id bigserial NOT NULL,
	slug text NOT NULL,
	zhtw_display_name text NOT NULL,
	english_display_name text NULL,
	category_type text NOT NULL,
	zhtw_description text NULL,
	"source" text NULL,
	source_url text NULL,
	is_active bool DEFAULT true NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	emoji text NULL,
	english_description text NULL,
	cover_img_url text NULL,
	CONSTRAINT check_valid_category_type CHECK ((category_type = ANY (ARRAY['label'::text, 'ranking'::text, 'award'::text, 'campaign'::text]))),
	CONSTRAINT check_valid_emoji_length CHECK (((emoji IS NULL) OR ((char_length(emoji) >= 1) AND (char_length(emoji) <= 8)))),
	CONSTRAINT property_mapping_pkey PRIMARY KEY (id),
	CONSTRAINT property_mapping_slug_key UNIQUE (slug)
);


-- public.users definition

-- Drop table

-- DROP TABLE public.users;

CREATE TABLE public.users (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	email text NULL,
	display_name text NULL,
	first_name text NULL,
	last_name text NULL,
	avatar_url text NULL,
	gender text NULL,
	provider text NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	found_us_source text NULL,
	CONSTRAINT check_valid_email_format CHECK (((email IS NULL) OR (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'::text))),
	CONSTRAINT check_valid_found_us_source CHECK (((found_us_source IS NULL) OR (found_us_source = ANY (ARRAY['instagram'::text, 'linkedin'::text, 'threads'::text, 'friends'::text, 'web_summit'::text, 'app_store'::text, 'organic'::text, 'other'::text])))),
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_created ON public.users USING btree (created_at);
CREATE INDEX idx_users_created_at ON public.users USING btree (created_at);
CREATE INDEX idx_users_email ON public.users USING btree (email);
CREATE INDEX idx_users_found_us_source ON public.users USING btree (found_us_source);


-- public.user_contents definition

-- Drop table

-- DROP TABLE public.user_contents;

CREATE TABLE public.user_contents (
	user_id uuid NOT NULL,
	content_id uuid NOT NULL,
	status text DEFAULT 'pending'::text NOT NULL,
	is_deleted bool DEFAULT false NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT user_contents_pkey PRIMARY KEY (user_id, content_id),
	CONSTRAINT user_contents_content_id_fkey FOREIGN KEY (content_id) REFERENCES public.contents(id),
	CONSTRAINT user_contents_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);
CREATE INDEX idx_user_contents_created_at ON public.user_contents USING btree (created_at);
CREATE INDEX idx_user_contents_user_id ON public.user_contents USING btree (user_id);