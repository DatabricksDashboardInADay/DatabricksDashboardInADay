-- Silver: employee (business object)
-- Employees with their manager (manager_id). Together they form the org chart.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}employee
COMMENT 'Employees incl. their manager (org chart). store_id is NULL for head-office staff.'
AS
SELECT
    CAST(employee_id AS INT)            AS employee_id,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS full_name,
    job_title,
    CAST(store_id AS INT)               AS store_id,
    CAST(manager_id AS INT)             AS manager_id,
    CAST(hired_on AS DATE)              AS hired_on
FROM bronze.${prefix}hr_employees;
