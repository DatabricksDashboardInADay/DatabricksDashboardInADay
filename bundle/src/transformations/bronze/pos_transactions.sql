-- Bronze: POS (café cash registers) – pos_transactions
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}pos_transactions
COMMENT 'Raw POS transaction headers: one row per café receipt, including voided receipts.'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}pos_transactions/',
    format => 'csv',
    header => true
);
