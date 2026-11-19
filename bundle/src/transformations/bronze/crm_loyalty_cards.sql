-- Bronze: CRM – crm_loyalty_cards
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}crm_loyalty_cards
COMMENT 'Raw loyalty program cards. A customer can have more than one card over time.'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}crm_loyalty_cards/',
    format => 'csv',
    header => true
);
