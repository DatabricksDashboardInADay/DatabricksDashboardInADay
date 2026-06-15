-- Ingests the date dimension CSV from the bronze volume into a silver
-- streaming table using Auto Loader.  Each row represents one calendar day
-- with pre-computed helper columns (year, month, day-of-week, season, etc.).

CREATE OR REFRESH STREAMING TABLE silver.${prefix}dim_date AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/${prefix}dim_date/',
  format => 'csv'
);
