-- Promotes the silver customer dimension into the gold layer as a
-- materialized view, making it directly queryable for dashboards and
-- reporting.

CREATE OR REPLACE MATERIALIZED VIEW ${gold_schema}.dim_customer AS
SELECT * FROM ${silver_schema}.dim_customer;
