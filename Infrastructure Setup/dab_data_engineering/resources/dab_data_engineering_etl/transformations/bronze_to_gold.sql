-- =======================================
-- SILVER: Streaming tables (Auto Loader)
-- =======================================

-- Date dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_date AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_date/',
  format => 'csv'
);

-- Store dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_store AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_store/',
  format => 'csv'
);

-- Customer dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_customer AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_customer/',
  format => 'csv'
);

-- Product dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_product AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_product/',
  format => 'csv'
);

-- Coffee sales fact (Parquet -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.fact_coffee_sales AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/fact_coffee_sales/',
  format => 'parquet'
);

-- =======================================
-- GOLD: Materialized views on SILVER
-- =======================================

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_date AS
SELECT * FROM silver.dim_date;

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_store AS
SELECT * FROM silver.dim_store;

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_customer AS
SELECT * FROM silver.dim_customer;

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_product AS
SELECT * FROM silver.dim_product;

CREATE OR REPLACE MATERIALIZED VIEW gold.fact_coffee_sales AS
SELECT
    fcs.*,
    dp.list_price_usd * fcs.quantity_sold                       AS gross_revenue_usd,
    (dp.list_price_usd * fcs.quantity_sold) / (1 + ds.tax_rate) AS net_revenue_usd,
    1 AS vat_usd,
    1 AS cost_of_goods_usd
FROM silver.fact_coffee_sales fcs
JOIN silver.dim_product dp
  ON fcs.product_key = dp.product_key
JOIN silver.dim_store ds
  ON fcs.store_key = ds.store_key;