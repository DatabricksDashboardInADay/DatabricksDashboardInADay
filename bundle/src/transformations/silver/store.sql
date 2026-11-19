-- Silver: store (business object)
-- Cafés and the online shop with typed attributes.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}store
COMMENT 'Stores (five cafés in German cities and the online shop).'
AS
SELECT
    CAST(store_id AS INT)               AS store_id,
    store_name,
    store_type,
    city,
    neighborhood_or_channel,
    CAST(opened_on AS DATE)             AS opened_on,
    CAST(is_online AS BOOLEAN)          AS is_online,
    CAST(store_area_sqm AS DOUBLE)      AS store_area_sqm,
    CAST(seating_capacity AS INT)       AS seating_capacity,
    CAST(manager_employee_id AS INT)    AS manager_employee_id,
    country_name,
    country_iso2,
    country_iso3,
    state_province,
    state_iso2,
    county_district,
    CAST(postal_code AS STRING)         AS postal_code,
    street_address,
    CAST(latitude AS DOUBLE)            AS latitude,
    CAST(longitude AS DOUBLE)           AS longitude
FROM bronze.${prefix}erp_stores;
