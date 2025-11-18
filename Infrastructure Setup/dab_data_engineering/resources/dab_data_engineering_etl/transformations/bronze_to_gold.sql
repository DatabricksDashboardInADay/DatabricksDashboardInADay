-- ====================
-- SILVER: Streaming tables (Auto Loader)
-- ====================

-- dim_date (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE sunny_bay_roastery.silver.dim_date AS
SELECT
  *
FROM STREAM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_date/",
  format => "csv"
);

-- dim_store (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE sunny_bay_roastery.silver.dim_store AS
SELECT
  *
FROM STREAM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_store/",
  format => "csv"
);

-- dim_customer (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE sunny_bay_roastery.silver.dim_customer AS
SELECT
  *
FROM STREAM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_customer/",
  format => "csv"
);

-- dim_product (CSV -> silver streaming)
CREATE OR REFRESH STREAMING TABLE sunny_bay_roastery.silver.dim_product AS
SELECT
  *
FROM STREAM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_product/",
  format => "csv"
);

-- fact_coffee_sales (Parquet -> silver streaming)
CREATE OR REFRESH STREAMING TABLE sunny_bay_roastery.silver.fact_coffee_sales AS
SELECT
  *
FROM STREAM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/fact_coffee_sales/",
  format => "parquet"
);

-- ====================
-- GOLD: Tables on top of silver
-- ====================

CREATE OR REFRESH TABLE sunny_bay_roastery.gold.dim_date AS
SELECT * FROM sunny_bay_roastery.silver.dim_date;

CREATE OR REFRESH TABLE sunny_bay_roastery.gold.dim_store AS
SELECT * FROM sunny_bay_roastery.silver.dim_store;

CREATE OR REFRESH TABLE sunny_bay_roastery.gold.dim_customer AS
SELECT * FROM sunny_bay_roastery.silver.dim_customer;

CREATE OR REFRESH TABLE sunny_bay_roastery.gold.dim_product AS
SELECT * FROM sunny_bay_roastery.silver.dim_product;

CREATE OR REFRESH TABLE sunny_bay_roastery.gold.fact_coffee_sales AS
SELECT * FROM sunny_bay_roastery.silver.fact_coffee_sales;
