CREATE OR REPLACE FUNCTION dv.norm_text(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT case when t is null then null else upper(btrim(t)) end;
$$;

CREATE OR REPLACE FUNCTION dv.md5_char32(t text)
RETURNS char(32)
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT md5(coalesce(t,''))::char(32);
$$;

CREATE TABLE IF NOT EXISTS dv.h_ship_mode (
    ship_mode_hk char(32) PRIMARY KEY,
    bk_ship_mode text NOT NULL,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (ship_mode_hk);

CREATE TABLE IF NOT EXISTS dv.h_segment (
    segment_hk char(32) PRIMARY KEY,
    bk_segment text NOT NULL,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (segment_hk);

CREATE TABLE IF NOT EXISTS dv.h_geography (
    geography_hk char(32) PRIMARY KEY,
    bk_country text,
    bk_state text,
    bk_city text,
    bk_postal_code text,
    bk_region text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (geography_hk);

CREATE TABLE IF NOT EXISTS dv.h_category (
    category_hk char(32) PRIMARY KEY,
    bk_category text NOT NULL,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (category_hk);

CREATE TABLE IF NOT EXISTS dv.h_sub_category (
    sub_category_hk char(32) PRIMARY KEY,
    bk_sub_category text NOT NULL,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (sub_category_hk);

CREATE TABLE IF NOT EXISTS dv.s_ship_mode (
    ship_mode_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    ship_mode_text text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (ship_mode_hk);

CREATE TABLE IF NOT EXISTS dv.s_segment (
    segment_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    segment_text text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (segment_hk);

CREATE TABLE IF NOT EXISTS dv.s_geography (
    geography_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    country text,
    state text,
    city text,
    postal_code text,
    region text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (geography_hk);

CREATE TABLE IF NOT EXISTS dv.s_category (
    category_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    category_text text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (category_hk);

CREATE TABLE IF NOT EXISTS dv.s_sub_category (
    sub_category_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    sub_category_text text,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (sub_category_hk);
