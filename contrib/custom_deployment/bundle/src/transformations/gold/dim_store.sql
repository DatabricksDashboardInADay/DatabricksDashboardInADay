-- Promotes the silver store dimension into the gold layer as a materialized
-- view, making it directly queryable for dashboards and reporting.

CREATE OR REPLACE MATERIALIZED VIEW ${gold_schema}.${user_name}_dim_store AS
SELECT * FROM ${silver_schema}.${user_name}_dim_store;
