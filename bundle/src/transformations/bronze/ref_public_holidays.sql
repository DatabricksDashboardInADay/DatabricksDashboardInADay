-- Bronze: Reference data – ref_public_holidays
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}ref_public_holidays
COMMENT 'German public holidays that apply in all federal states (Bundesländer).'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}ref_public_holidays/',
    format => 'csv',
    header => true
);
