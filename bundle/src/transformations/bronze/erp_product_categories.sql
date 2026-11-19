-- Bronze: ERP – erp_product_categories
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}erp_product_categories
COMMENT 'Raw product category tree (parent/child).'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}erp_product_categories/',
    format => 'csv',
    header => true
);
