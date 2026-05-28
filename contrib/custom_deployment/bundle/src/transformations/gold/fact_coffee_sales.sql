-- Enriches the silver fact table with calculated revenue and cost columns
-- by joining the product and store dimensions.  This is the main analytical
-- table used by dashboards and metric views.

CREATE OR REPLACE MATERIALIZED VIEW ${gold_schema}.${user_name}_fact_coffee_sales
CLUSTER BY (store_key, date_key)
AS
SELECT
    fcs.*,
    dp.list_price_usd * fcs.quantity_sold                       AS gross_revenue_usd,
    (dp.list_price_usd * fcs.quantity_sold) / (1 + ds.tax_rate) AS net_revenue_usd,
    ds.tax_rate * dp.list_price_usd * fcs.quantity_sold  AS vat_usd,
    dp.cost_of_goods_usd * fcs.quantity_sold AS cost_of_goods_usd
FROM ${silver_schema}.${user_name}_fact_coffee_sales fcs
JOIN ${silver_schema}.${user_name}_dim_product dp
  ON fcs.product_key = dp.product_key
JOIN ${silver_schema}.${user_name}_dim_store ds
  ON fcs.store_key = ds.store_key;
