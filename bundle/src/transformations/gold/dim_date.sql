-- Promotes the silver date dimension into the gold layer as a materialized
-- view, making it directly queryable for dashboards and reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.${prefix}dim_date AS
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
FROM silver.${prefix}dim_date;
