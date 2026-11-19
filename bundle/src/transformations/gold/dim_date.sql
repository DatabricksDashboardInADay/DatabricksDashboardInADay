-- Gold: dim_date
-- A complete calendar (one row per day, full calendar years) generated with SEQUENCE + EXPLODE,
-- enriched with the German public holidays (LEFT JOIN: most days are not a holiday).

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_date
COMMENT 'Calendar dimension: one row per day covering all years with sales, incl. German public holidays.'
AS
WITH bounds AS (
    SELECT
        MAKE_DATE(YEAR(MIN(order_date)), 1, 1)   AS first_day,
        MAKE_DATE(YEAR(MAX(order_date)), 12, 31) AS last_day
    FROM silver.${prefix}sales_order
),
calendar AS (
    SELECT EXPLODE(SEQUENCE(first_day, last_day, INTERVAL 1 DAY)) AS date
    FROM bounds
)
SELECT
    CAST(DATE_FORMAT(c.date, 'yyyyMMdd') AS INT)  AS date_key,
    c.date,
    YEAR(c.date)                                  AS year,
    MONTH(c.date)                                 AS month,
    DAY(c.date)                                   AS day,
    WEEKOFYEAR(c.date)                            AS calendar_week,
    WEEKDAY(c.date) + 1                           AS day_of_week,   -- 1 = Monday ... 7 = Sunday
    DATE_FORMAT(c.date, 'E')                      AS day_name,
    WEEKDAY(c.date) >= 5                          AS is_weekend,
    CASE
        WHEN MONTH(c.date) IN (12, 1, 2) THEN 'winter'
        WHEN MONTH(c.date) IN (3, 4, 5)  THEN 'spring'
        WHEN MONTH(c.date) IN (6, 7, 8)  THEN 'summer'
        ELSE 'autumn'
    END                                           AS season,
    h.holiday_date IS NOT NULL                    AS is_public_holiday,
    h.holiday_name
FROM calendar AS c
LEFT JOIN silver.${prefix}public_holiday AS h
    ON c.date = h.holiday_date;
