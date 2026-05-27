-- Ingests the store dimension CSV from the bronze volume into a silver
-- streaming table.  Contains one row per store with location, capacity,
-- tax-rate and geographic attributes.

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.dim_store AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/${bronze_schema}/${volume}/dim_store/',
  format => 'csv',
  rescuedDataColumn => 'None'
);
