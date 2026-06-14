-- Promotes the silver product dimension into the gold layer as a
-- materialized view, making it directly queryable for dashboards and
-- reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_product AS
SELECT
    product_key,
    product_name,
    product_category,
    product_subcategory,
    is_beans,
    available_in_store,
    available_online,
    list_price_usd,
    cost_of_goods_usd
FROM silver.dim_product;
