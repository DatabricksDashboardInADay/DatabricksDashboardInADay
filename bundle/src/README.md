This folder contains all source code of the Sunny Bay Roastery bundle:

- `data/`: Python modules that generate the raw source-system files (POS, webshop, CRM, ERP, HR, reference data) for a rolling 24-month sales window.
- `notebooks/`: Setup notebooks run by the job (generate data, deploy metadata, metric view and Genie agent).
- `transformations/`: The Spark Declarative Pipeline (SQL), one dataset per file:
  - `bronze/`: streaming tables that ingest the raw files from the Volume as-is
  - `silver/`: materialized views that clean and integrate the sources into business objects
  - `gold/`: materialized views that build the star schema (`fact_coffee_sales` + `dim_*`)
- `dashboards/`: AI/BI dashboard definitions.

## Getting Started

* By convention, every dataset under `transformations` is in a separate file.
  Read more about the syntax at https://docs.databricks.com/aws/en/ldp/developer/sql-ref.
* Use `Run file` to run and preview a single transformation.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.

For more tutorials and reference material, see https://docs.databricks.com/aws/en/ldp/.
