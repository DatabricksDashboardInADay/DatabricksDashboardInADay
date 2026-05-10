-- Ingests the customer dimension CSV from the bronze volume into a silver
-- streaming table.  Each row describes a customer with loyalty segment,
-- channel preference and city.

CREATE OR REFRESH STREAMING TABLE silver.dim_customer AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/dim_customer/',
  format => 'csv',
  rescuedDataColumn => 'None'
);
