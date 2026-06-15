-- Ingests the product dimension CSV from the bronze volume into a silver
-- streaming table.  Contains product name, category, pricing and
-- availability attributes for every item in the catalogue.

CREATE OR REFRESH STREAMING TABLE silver.${prefix}dim_product AS
SELECT
    *
FROM STREAM read_files(
  '/Volumes/${catalog}/bronze/raw/${prefix}dim_product/',
  format => 'csv'
);
