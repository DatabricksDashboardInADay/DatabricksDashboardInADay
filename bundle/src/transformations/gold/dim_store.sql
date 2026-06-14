-- Promotes the silver store dimension into the gold layer as a materialized
-- view, making it directly queryable for dashboards and reporting.

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
