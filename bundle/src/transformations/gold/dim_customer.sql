-- Promotes the silver customer dimension into the gold layer as a
-- materialized view, making it directly queryable for dashboards and
-- reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.${prefix}dim_customer AS
SELECT
    customer_key,
    loyalty_segment,
    channel_preference,
    is_home_barista,
    city
FROM silver.${prefix}dim_customer;
