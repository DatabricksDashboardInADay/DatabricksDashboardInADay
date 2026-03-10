-- Promotes the silver product dimension into the gold layer as a
-- materialized view, making it directly queryable for dashboards and
-- reporting.

CREATE OR REPLACE MATERIALIZED VIEW gold.dim_product AS
SELECT * FROM silver.dim_product;
