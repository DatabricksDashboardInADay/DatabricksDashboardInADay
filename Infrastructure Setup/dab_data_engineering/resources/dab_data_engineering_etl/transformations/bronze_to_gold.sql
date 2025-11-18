-- Date dimension (CSV -> bronze streaming)
CREATE OR REFRESH STREAMING TABLE bronze.date AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_date/',
  format => 'csv'
);

-- Store dimension (CSV -> bronze streaming)
CREATE OR REFRESH STREAMING TABLE bronze.store AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_store/',
  format => 'csv'
);

-- Customer dimension (CSV -> bronze streaming)
CREATE OR REFRESH STREAMING TABLE bronze.customer AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_customer/',
  format => 'csv'
);

-- Product dimension (CSV -> bronze streaming)
CREATE OR REFRESH STREAMING TABLE bronze.product AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/dim_product/',
  format => 'csv'
);

-- Coffee sales fact (Parquet -> bronze streaming)
CREATE OR REFRESH STREAMING TABLE bronze.coffee_sales AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/sunny_bay_roastery/bronze/raw/fact_coffee_sales/',
  format => 'parquet'
);

CREATE MATERIALIZED VIEW gold.dim_date AS
SELECT * FROM bronze.date;

CREATE MATERIALIZED VIEW gold.dim_store AS
SELECT * FROM bronze.store;

CREATE MATERIALIZED VIEW gold.dim_customer AS
SELECT * FROM bronze.customer;

CREATE MATERIALIZED VIEW gold.dim_product AS
SELECT * FROM bronze.product;

CREATE MATERIALIZED VIEW gold.fact_coffee_sales AS
SELECT
    fcs.*
    -- dp.list_price_usd * fcs.quantity_sold                               AS gross_revenue_usd,
    -- (dp.list_price_usd * fcs.quantity_sold) / (1 + ds.tax_rate)         AS net_revenue_usd
FROM bronze.coffee_sales fcs
JOIN bronze.product dp
  ON fcs.product_key = dp.product_key
JOIN bronze.store ds
  ON fcs.store_key = ds.store_key;