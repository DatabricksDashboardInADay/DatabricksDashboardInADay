-- Promotes the silver date dimension into the gold layer as a materialized
-- view, making it directly queryable for dashboards and reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_date AS
SELECT * FROM silver.dim_date;
