-- Promotes the silver store dimension into the gold layer as a materialized
-- view, making it directly queryable for dashboards and reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_store AS
SELECT * FROM silver.dim_store;
