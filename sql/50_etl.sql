WITH src AS (
  SELECT DISTINCT dv.norm_text(ship_mode) AS ship_mode_norm FROM stage.sample_superstore
)
INSERT INTO dv.h_ship_mode (ship_mode_hk, bk_ship_mode)
SELECT dv.md5_char32(ship_mode_norm), ship_mode_norm
FROM src s
LEFT JOIN dv.h_ship_mode h ON h.ship_mode_hk = dv.md5_char32(s.ship_mode_norm)
WHERE h.ship_mode_hk IS NULL;

WITH src AS (
  SELECT DISTINCT dv.norm_text(segment) AS segment_norm FROM stage.sample_superstore
)
INSERT INTO dv.h_segment (segment_hk, bk_segment)
SELECT dv.md5_char32(segment_norm), segment_norm
FROM src
LEFT JOIN dv.h_segment h ON h.segment_hk = dv.md5_char32(segment_norm)
WHERE h.segment_hk IS NULL;

WITH src AS (
  SELECT DISTINCT
    dv.norm_text(country)  AS country_norm,
    dv.norm_text(state)    AS state_norm,
    dv.norm_text(city)     AS city_norm,
    dv.norm_text(postal_code) AS postal_norm,
    dv.norm_text(region)   AS region_norm
  FROM stage.sample_superstore
)
INSERT INTO dv.h_geography (geography_hk, bk_country, bk_state, bk_city, bk_postal_code, bk_region)
SELECT dv.md5_char32(coalesce(country_norm,'')||'|'||coalesce(state_norm,'')||'|'||coalesce(city_norm,'')||'|'||coalesce(postal_norm,'')||'|'||coalesce(region_norm,'')),
       country_norm, state_norm, city_norm, postal_norm, region_norm
FROM src s
LEFT JOIN dv.h_geography h
  ON h.geography_hk = dv.md5_char32(coalesce(s.country_norm,'')||'|'||coalesce(s.state_norm,'')||'|'||coalesce(s.city_norm,'')||'|'||coalesce(s.postal_norm,'')||'|'||coalesce(s.region_norm,''))
WHERE h.geography_hk IS NULL;

WITH src AS (
  SELECT DISTINCT dv.norm_text(category) AS category_norm FROM stage.sample_superstore
)
INSERT INTO dv.h_category (category_hk, bk_category)
SELECT dv.md5_char32(category_norm), category_norm
FROM src s
LEFT JOIN dv.h_category h ON h.category_hk = dv.md5_char32(s.category_norm)
WHERE h.category_hk IS NULL;

WITH src AS (
  SELECT DISTINCT dv.norm_text(sub_category) AS sub_category_norm FROM stage.sample_superstore
)
INSERT INTO dv.h_sub_category (sub_category_hk, bk_sub_category)
SELECT dv.md5_char32(sub_category_norm), sub_category_norm
FROM src s
LEFT JOIN dv.h_sub_category h ON h.sub_category_hk = dv.md5_char32(s.sub_category_norm)
WHERE h.sub_category_hk IS NULL;

WITH src AS (
  SELECT DISTINCT dv.md5_char32(dv.norm_text(ship_mode)) AS ship_mode_hk,
         dv.norm_text(ship_mode) AS ship_mode_text,
         dv.md5_char32(coalesce(dv.norm_text(ship_mode),'') ) AS hd
  FROM stage.sample_superstore
)
INSERT INTO dv.s_ship_mode (ship_mode_hk, hashdiff, ship_mode_text)
SELECT ship_mode_hk, hd, ship_mode_text
FROM src s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_ship_mode sm WHERE sm.ship_mode_hk = s.ship_mode_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;

WITH src AS (
  SELECT DISTINCT dv.md5_char32(dv.norm_text(segment)) AS segment_hk,
         dv.norm_text(segment) AS segment_text,
         dv.md5_char32(coalesce(dv.norm_text(segment),'') ) AS hd
  FROM stage.sample_superstore
)
INSERT INTO dv.s_segment (segment_hk, hashdiff, segment_text)
SELECT segment_hk, hd, segment_text
FROM src s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_segment ss WHERE ss.segment_hk = s.segment_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;

WITH src AS (
  SELECT DISTINCT
    dv.md5_char32(coalesce(dv.norm_text(country),'')||'|'||coalesce(dv.norm_text(state),'')||'|'||coalesce(dv.norm_text(city),'')||'|'||coalesce(dv.norm_text(postal_code),'')||'|'||coalesce(dv.norm_text(region),'')) AS geography_hk,
    dv.norm_text(country)    AS country,
    dv.norm_text(state)      AS state,
    dv.norm_text(city)       AS city,
    dv.norm_text(postal_code) AS postal_code,
    dv.norm_text(region)     AS region,
    dv.md5_char32(coalesce(dv.norm_text(country),'')||'|'||coalesce(dv.norm_text(state),'')||'|'||coalesce(dv.norm_text(city),'')||'|'||coalesce(dv.norm_text(postal_code),'')||'|'||coalesce(dv.norm_text(region),'')) AS hd
  FROM stage.sample_superstore
)
INSERT INTO dv.s_geography (geography_hk, hashdiff, country, state, city, postal_code, region)
SELECT geography_hk, hd, country, state, city, postal_code, region
FROM src s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_geography sg WHERE sg.geography_hk = s.geography_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;

WITH src AS (
  SELECT DISTINCT dv.md5_char32(dv.norm_text(category)) AS category_hk,
         dv.norm_text(category) AS category_text,
         dv.md5_char32(coalesce(dv.norm_text(category),'') ) AS hd
  FROM stage.sample_superstore
)
INSERT INTO dv.s_category (category_hk, hashdiff, category_text)
SELECT category_hk, hd, category_text
FROM src s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_category sc WHERE sc.category_hk = s.category_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;

WITH src AS (
  SELECT DISTINCT dv.md5_char32(dv.norm_text(sub_category)) AS sub_category_hk,
         dv.norm_text(sub_category) AS sub_category_text,
         dv.md5_char32(coalesce(dv.norm_text(sub_category),'') ) AS hd
  FROM stage.sample_superstore
)
INSERT INTO dv.s_sub_category (sub_category_hk, hashdiff, sub_category_text)
SELECT sub_category_hk, hd, sub_category_text
FROM src s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_sub_category ss WHERE ss.sub_category_hk = s.sub_category_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;

WITH src AS (
  SELECT
    dv.md5_char32(dv.norm_text(ship_mode)) AS ship_mode_hk,
    dv.md5_char32(dv.norm_text(segment))   AS segment_hk,
    dv.md5_char32(coalesce(dv.norm_text(country),'')||'|'||coalesce(dv.norm_text(state),'')||'|'||coalesce(dv.norm_text(city),'')||'|'||coalesce(dv.norm_text(postal_code),'')||'|'||coalesce(dv.norm_text(region),'')) AS geography_hk,
    dv.md5_char32(dv.norm_text(category))  AS category_hk,
    dv.md5_char32(dv.norm_text(sub_category)) AS sub_category_hk,
    sales, quantity, discount, profit
  FROM stage.sample_superstore
)
, lk AS (
  SELECT dv.md5_char32(ship_mode_hk||segment_hk||geography_hk||category_hk||sub_category_hk) AS sale_hk,
         ship_mode_hk, segment_hk, geography_hk, category_hk, sub_category_hk,
         sales, quantity, discount, profit
  FROM src
)
INSERT INTO dv.l_sale (sale_hk, ship_mode_hk, segment_hk, geography_hk, category_hk, sub_category_hk)
SELECT DISTINCT l.sale_hk, l.ship_mode_hk, l.segment_hk, l.geography_hk, l.category_hk, l.sub_category_hk
FROM lk l
LEFT JOIN dv.l_sale t ON t.sale_hk = l.sale_hk
WHERE t.sale_hk IS NULL;

WITH src AS (
  SELECT
    dv.md5_char32(dv.norm_text(ship_mode)) AS ship_mode_hk,
    dv.md5_char32(dv.norm_text(segment))   AS segment_hk,
    dv.md5_char32(coalesce(dv.norm_text(country),'')||'|'||coalesce(dv.norm_text(state),'')||'|'||coalesce(dv.norm_text(city),'')||'|'||coalesce(dv.norm_text(postal_code),'')||'|'||coalesce(dv.norm_text(region),'')) AS geography_hk,
    dv.md5_char32(dv.norm_text(category))  AS category_hk,
    dv.md5_char32(dv.norm_text(sub_category)) AS sub_category_hk,
    sales, quantity, discount, profit
  FROM stage.sample_superstore
)
, lk AS (
  SELECT dv.md5_char32(ship_mode_hk||segment_hk||geography_hk||category_hk||sub_category_hk) AS sale_hk,
         sales, quantity, discount, profit,
         dv.md5_char32(coalesce(sales::text,'')||'|'||coalesce(quantity::text,'')||'|'||coalesce(discount::text,'')||'|'||coalesce(profit::text,'')) AS hd
  FROM src
)
INSERT INTO dv.s_sale_metrics (sale_hk, hashdiff, sales, quantity, discount, profit)
SELECT sale_hk, hd, sales, quantity, discount, profit
FROM lk s
LEFT JOIN LATERAL (
  SELECT hashdiff FROM dv.s_sale_metrics sm WHERE sm.sale_hk = s.sale_hk ORDER BY load_dts DESC LIMIT 1
) latest ON true
WHERE latest.hashdiff IS DISTINCT FROM s.hd;


