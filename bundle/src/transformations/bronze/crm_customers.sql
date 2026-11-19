-- Bronze: CRM – crm_customers
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}crm_customers
COMMENT 'Raw CRM customer extract. Contains several versions (history rows) per customer.'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}crm_customers/',
    format => 'csv',
    header => true
);
