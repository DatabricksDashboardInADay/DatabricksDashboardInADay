-- Bronze: ERP – erp_product_prices
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}erp_product_prices
COMMENT 'Raw product price history: each row is valid from valid_from until the next price change.'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}erp_product_prices/',
    format => 'csv',
    header => true
);
