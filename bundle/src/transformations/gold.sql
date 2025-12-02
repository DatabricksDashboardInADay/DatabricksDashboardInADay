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
    ds.tax_rate * dp.list_price_usd * fcs.quantity_sold  AS vat_usd,
    dp.cost_of_goods_usd * fcs.quantity_sold AS cost_of_goods_usd
FROM silver.fact_coffee_sales fcs
JOIN silver.dim_product dp
  ON fcs.product_key = dp.product_key
JOIN silver.dim_store ds
  ON fcs.store_key = ds.store_key;