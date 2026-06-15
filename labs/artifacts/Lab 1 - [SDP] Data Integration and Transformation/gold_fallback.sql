-- =======================================
-- GOLD: Materialized views on SILVER
-- =======================================

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_date AS
SELECT
    date_key,
    date,
    year,
    month,
    day,
    calendar_week,
    day_of_week,
    day_name,
    is_weekend,
    season,
    is_us_public_holiday
FROM silver.dim_date;

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_store AS
SELECT
    store_key,
    store_name,
    store_type,
    city,
    neighborhood_or_channel,
    is_online,
    store_area_sqm,
    seating_capacity,
    num_employees,
    store_manager,
    tax_rate,
    country_name,
    country_iso2,
    country_iso3,
    state_province,
    state_iso2,
    county_district,
    postal_code,
    latitude,
    longitude
FROM silver.dim_store;

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_customer AS
SELECT
    customer_key,
    loyalty_segment,
    channel_preference,
    is_home_barista,
    city
FROM silver.dim_customer;

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

CREATE OR REPLACE MATERIALIZED VIEW gold.fact_coffee_sales AS
SELECT
    fcs.date_key,
    fcs.store_key,
    fcs.product_key,
    fcs.customer_key,
    fcs.quantity_sold,
    dp.list_price_usd * fcs.quantity_sold                       AS gross_revenue_usd,
    (dp.list_price_usd * fcs.quantity_sold) / (1 + ds.tax_rate) AS net_revenue_usd,
    (dp.list_price_usd * fcs.quantity_sold) * 1.1 AS gross_revenue_eur,
    ds.tax_rate * dp.list_price_usd * fcs.quantity_sold  AS vat_usd,
    dp.cost_of_goods_usd * fcs.quantity_sold AS cost_of_goods_usd
FROM silver.fact_coffee_sales fcs
JOIN silver.dim_product dp
  ON fcs.product_key = dp.product_key
JOIN silver.dim_store ds
  ON fcs.store_key = ds.store_key;

CREATE OR REFRESH MATERIALIZED VIEW gold.total_revenue_by_year AS
SELECT
    store_key AS store_key,
    SUM(gross_revenue_usd) AS total_gross_revenue_usd
FROM gold.fact_coffee_sales
GROUP BY store_key;