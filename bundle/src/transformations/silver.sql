-- =======================================
-- SILVER: Streaming tables (Auto Loader)
-- =======================================

-- Date dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_date AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/dim_date/',
  format => 'csv'
);

-- Store dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_store AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/dim_store/',
  format => 'csv'
);

-- Customer dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_customer AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/dim_customer/',
  format => 'csv'
);

-- Product dimension (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.dim_product AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/dim_product/',
  format => 'csv'
);

-- Coffee sales fact (Parquet -> silver streaming)
CREATE OR REFRESH STREAMING TABLE silver.fact_coffee_sales AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/fact_coffee_sales/',
  format => 'parquet'
);
