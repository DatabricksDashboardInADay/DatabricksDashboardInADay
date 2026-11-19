-- Bronze: HR – hr_employees
-- Ingests the raw files from the Volume as-is (append-only streaming table).
-- Only lineage columns are added; all cleaning happens in the silver layer.

CREATE OR REFRESH STREAMING TABLE bronze.${prefix}hr_employees
COMMENT 'Raw employee list incl. the manager of every employee (org chart).'
AS
SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bronze/raw/${prefix}hr_employees/',
    format => 'csv',
    header => true
);
