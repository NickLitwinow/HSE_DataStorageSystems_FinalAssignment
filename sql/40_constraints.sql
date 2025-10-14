DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_ship_mode_h') THEN
    ALTER TABLE dv.s_ship_mode
      ADD CONSTRAINT fk_s_ship_mode_h
      FOREIGN KEY (ship_mode_hk) REFERENCES dv.h_ship_mode(ship_mode_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_segment_h') THEN
    ALTER TABLE dv.s_segment
      ADD CONSTRAINT fk_s_segment_h
      FOREIGN KEY (segment_hk) REFERENCES dv.h_segment(segment_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_geography_h') THEN
    ALTER TABLE dv.s_geography
      ADD CONSTRAINT fk_s_geography_h
      FOREIGN KEY (geography_hk) REFERENCES dv.h_geography(geography_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_category_h') THEN
    ALTER TABLE dv.s_category
      ADD CONSTRAINT fk_s_category_h
      FOREIGN KEY (category_hk) REFERENCES dv.h_category(category_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_sub_category_h') THEN
    ALTER TABLE dv.s_sub_category
      ADD CONSTRAINT fk_s_sub_category_h
      FOREIGN KEY (sub_category_hk) REFERENCES dv.h_sub_category(sub_category_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_l_sale_ship_mode') THEN
    ALTER TABLE dv.l_sale
      ADD CONSTRAINT fk_l_sale_ship_mode FOREIGN KEY (ship_mode_hk) REFERENCES dv.h_ship_mode(ship_mode_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_l_sale_segment') THEN
    ALTER TABLE dv.l_sale
      ADD CONSTRAINT fk_l_sale_segment FOREIGN KEY (segment_hk) REFERENCES dv.h_segment(segment_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_l_sale_geography') THEN
    ALTER TABLE dv.l_sale
      ADD CONSTRAINT fk_l_sale_geography FOREIGN KEY (geography_hk) REFERENCES dv.h_geography(geography_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_l_sale_category') THEN
    ALTER TABLE dv.l_sale
      ADD CONSTRAINT fk_l_sale_category FOREIGN KEY (category_hk) REFERENCES dv.h_category(category_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_l_sale_sub_category') THEN
    ALTER TABLE dv.l_sale
      ADD CONSTRAINT fk_l_sale_sub_category FOREIGN KEY (sub_category_hk) REFERENCES dv.h_sub_category(sub_category_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_s_sale_metrics_l') THEN
    ALTER TABLE dv.s_sale_metrics
      ADD CONSTRAINT fk_s_sale_metrics_l FOREIGN KEY (sale_hk) REFERENCES dv.l_sale(sale_hk) NOT VALID;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_ship_mode_hk') THEN
    CREATE INDEX ix_s_ship_mode_hk ON dv.s_ship_mode (ship_mode_hk, load_dts DESC);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_segment_hk') THEN
    CREATE INDEX ix_s_segment_hk ON dv.s_segment (segment_hk, load_dts DESC);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_geography_hk') THEN
    CREATE INDEX ix_s_geography_hk ON dv.s_geography (geography_hk, load_dts DESC);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_category_hk') THEN
    CREATE INDEX ix_s_category_hk ON dv.s_category (category_hk, load_dts DESC);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_sub_category_hk') THEN
    CREATE INDEX ix_s_sub_category_hk ON dv.s_sub_category (sub_category_hk, load_dts DESC);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_s_sale_metrics_hk') THEN
    CREATE INDEX ix_s_sale_metrics_hk ON dv.s_sale_metrics (sale_hk, load_dts DESC);
  END IF;
END $$;