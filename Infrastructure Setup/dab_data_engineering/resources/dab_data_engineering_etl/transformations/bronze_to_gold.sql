-- Create a materialized view for the date dimension from CSV files
CREATE VIEW dim_date AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_date/",
  format => "csv"
);

-- Create a view for the store dimension from CSV files
CREATE VIEW dim_store AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_store/",
  format => "csv"
);

-- Create a view for the customer dimension from CSV files
CREATE VIEW dim_customer AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_customer/",
  format => "csv"
);

-- Create a view for the product dimension from CSV files
CREATE VIEW dim_product AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/dim_product/",
  format => "csv"
);

-- Create a view for the coffee sales fact table from Parquet files
CREATE VIEW fact_coffee_sales AS
SELECT
    *
FROM read_files(
  "/Volumes/sunny_bay_roastery/bronze/raw/fact_coffee_sales/",
  format => "parquet"
);


--------------------------------------
CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_date AS
SELECT * FROM dim_date;

CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_store AS
SELECT * FROM dim_store ds;

CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_customer AS
SELECT * FROM dim_customer;

CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.dim_product AS
SELECT * FROM dim_product;

CREATE MATERIALIZED VIEW sunny_bay_roastery.gold.fact_coffee_sales AS
SELECT
    fcs.*,
    dp.list_price_usd * fcs.quantity_sold AS gross_revenue_usd,
    (dp.list_price_usd * fcs.quantity_sold) / (1 + ds.tax_rate) AS net_revenue_usd
FROM fact_coffee_sales fcs
JOIN dim_product dp
  ON fcs.product_key = dp.product_key
JOIN dim_store ds
  ON fcs.store_key = ds.store_key;