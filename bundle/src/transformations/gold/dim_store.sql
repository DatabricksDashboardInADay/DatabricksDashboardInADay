-- Gold: dim_store
-- Stores enriched with data from the HR system: the name of the store manager and the
-- number of employees (LEFT JOINs, so a store without HR data would still be kept).

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_store
COMMENT 'Store dimension: five cafés in German cities and the online shop, incl. manager and head count.'
AS
WITH staff AS (
    SELECT store_id, COUNT(*) AS num_employees
    FROM silver.${prefix}employee
    WHERE store_id IS NOT NULL
    GROUP BY store_id
)
SELECT
    s.store_id                         AS store_key,
    s.store_name,
    s.store_type,
    s.city,
    s.neighborhood_or_channel,
    s.is_online,
    s.opened_on,
    s.store_area_sqm,
    s.seating_capacity,
    COALESCE(st.num_employees, 0)      AS num_employees,
    m.full_name                        AS store_manager,
    s.country_name,
    s.country_iso2,
    s.country_iso3,
    s.state_province,
    s.state_iso2,
    s.county_district,
    s.postal_code,
    s.latitude,
    s.longitude
FROM silver.${prefix}store AS s
LEFT JOIN silver.${prefix}employee AS m
    ON s.manager_employee_id = m.employee_id
LEFT JOIN staff AS st
    ON s.store_id = st.store_id;
