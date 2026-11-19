-- Silver: public_holiday (reference data)
-- German public holidays that apply in all federal states. The cafés are closed on these days.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}public_holiday
COMMENT 'German public holidays valid in all federal states (Bundesländer). Cafés are closed on these days.'
AS
SELECT
    CAST(holiday_date AS DATE) AS holiday_date,
    holiday_name
FROM bronze.${prefix}ref_public_holidays;
