CREATE TABLE IF NOT EXISTS dv.l_sale (
    sale_hk char(32) PRIMARY KEY,
    ship_mode_hk char(32) NOT NULL,
    segment_hk char(32) NOT NULL,
    geography_hk char(32) NOT NULL,
    category_hk char(32) NOT NULL,
    sub_category_hk char(32) NOT NULL,
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (sale_hk);

CREATE TABLE IF NOT EXISTS dv.s_sale_metrics (
    sale_hk char(32) NOT NULL,
    hashdiff char(32) NOT NULL,
    sales numeric(18,4),
    quantity integer,
    discount numeric(9,4),
    profit numeric(18,4),
    load_dts timestamp without time zone NOT NULL DEFAULT now(),
    record_source text NOT NULL DEFAULT 'SampleSuperstore.csv'
)
DISTRIBUTED BY (sale_hk);
