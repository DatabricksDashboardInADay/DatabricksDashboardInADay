-- Bronze: Webshop – web_orders
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}web_orders
COMMENT 'Raw web shop orders as nested JSON: one document per order with an array of items.'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}web_orders/',
    format => 'json'
);
