-- Ingests the coffee-sales fact Parquet files from the bronze volume into a
-- silver streaming table.  Each row is one transaction line containing
-- date_key, store_key, product_key, customer_key and quantity_sold.

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.fact_coffee_sales
AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/${bronze_schema}/${volume}/fact_coffee_sales/',
  format => 'parquet',
  rescuedDataColumn => 'None'
);
