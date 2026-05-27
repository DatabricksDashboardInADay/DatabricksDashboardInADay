-- Ingests the customer dimension CSV from the bronze volume into a silver
-- streaming table.  Each row describes a customer with loyalty segment,
-- channel preference and city.

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.dim_customer AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/${bronze_schema}/${volume}/dim_customer/',
  format => 'csv',
  rescuedDataColumn => 'None'
);
