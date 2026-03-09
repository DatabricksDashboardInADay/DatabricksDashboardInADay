# SQL Analyst Lab Takeover Guide

This document captures the current takeover state for the SQL-focused lab branch and defines the implementation path for the SQL Analyst lab track.

## Branch Baseline

- Colleague source branch: `origin/sql-lab`
- Personal continuation branch: `adam/sql-lab-takeover`
- Current implementation scope in SQL branch:
  - New SQL notebooks:
    - `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Bronze Tables.dbquery.ipynb`
    - `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Silver Tables.dbquery.ipynb`
    - `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Gold Tables.dbquery.ipynb`
  - SQL transformation parity changes (SDP path, not SQL-lab-specific):
    - `bundle/src/transformations/silver.sql` (DQ expectation for `quantity_sold > 0`)
    - `bundle/src/transformations/gold.sql` (EUR metric + aggregate view)

## Colleague Code Audit (Completed on Takeover)

The following issues were found in the original `origin/sql-lab` branch and fixed on `adam/sql-lab-takeover`:

| # | File | Issue | Fix |
|---|------|-------|-----|
| 1 | `bundle/databricks.yml` | Warehouse hardcoded to `nal-serverless-sql` (colleague-specific) | Reverted to `Serverless Starter Warehouse` |
| 2 | `Create Bronze Tables` | Table names used `customer`, `date`, `product`, `store`, `sales` instead of `dim_*`/`fact_*` | Renamed to `dim_customer`, `dim_date`, `dim_product`, `dim_store`, `fact_coffee_sales` |
| 3 | `Create Bronze Tables` | Parquet `read_files` included `header => true, inferSchema => true` (CSV-only options) | Removed; Parquet is self-describing |
| 4 | `Create Bronze Tables` | Notebook metadata contained leaked values (`catalog: bhambs`, `schema: construction_ai`) | Fixed to `sunny_bay_roastery` / `bronze` |
| 5 | `Create Silver Tables` | Used `s_date`, `s_store`, etc. naming and inconsistent schema qualification (some unqualified) | Renamed to `dim_*`/`fact_*`, fully qualified with `sunny_bay_roastery.silver.*` |
| 6 | `Create Gold Tables` | Used `d_date`, `d_store`, `f_coffee_sales` naming; references to `s_*` silver tables | Renamed to `dim_*`/`fact_*` to match metric view and dashboard expectations |
| 7 | `data/init.ipynb` | Databricks runtime metadata noise (cellMetadata, widget state, indent prefs) | Reverted to `origin/main` version (no actual code changes) |

## SQL Analyst Lab Track — Complete Flow

```
Lab 0 (Setup) → Lab 1 [SQL] → Lab 2 [SQL] → Lab 3 [SQL] → Lab 2 → Lab 3 → Lab 4
                 Medallion      Views &        Alerts &       Metric   Dash-   Genie
                 (SQL)          Assistant       Self-Service   Views    boards
```

## Gap Analysis Against Brief

### Covered

| Brief Requirement | Coverage | Lab |
|---|---|---|
| SQL Serverless / Pro Warehouse | Prerequisites and setup guidance | Lab 1 [SQL] |
| End-to-end SQL workflows | Bronze → Silver → Gold via SQL Editor | Lab 1 [SQL] |
| Analyst CUJ: queries | Exploratory queries, saved queries | Lab 1 [SQL], Lab 2 [SQL] |
| Analyst CUJ: views | `CREATE VIEW` exercises (3 business views) | Lab 2 [SQL] |
| Analyst CUJ: alerts | SQL Alert with threshold monitoring | Lab 3 [SQL] |
| Scheduled queries | Recurring query with notification | Lab 3 [SQL] |
| Databricks Assistant | 4 structured exercises (generate, refine, explain, optimise) | Lab 2 [SQL] |
| CSV/Excel upload | Upload sample CSV, join to gold, create comparison view | Lab 3 [SQL] |
| Databricks One | Guided exploration of unified home experience | Lab 3 [SQL] |
| Lakeflow Designer (optional) | Visual walkthrough of existing pipeline, no Spark required | Lab 2 [SQL] |
| Metric Views | Reused from shared Lab 2 | Lab 2 (shared) |
| AI/BI Dashboards | Reused from shared Lab 3 | Lab 3 (shared) |
| Genie | Reused from shared Lab 4 | Lab 4 (shared) |
| Facilitator decision points | Routing guidance embedded in each [SQL] lab + this doc | All [SQL] labs |
| GenAI modular composition | Self-contained modules with clear entry/exit, no cross-dependencies | Structural |

### Remaining (v2 / future)

- Advanced alert patterns (multi-condition, webhook integrations).
- Dedicated Lakeflow Designer hands-on exercise (create a pipeline visually).
- Formal GenAI prompt library for lab recomposition.
- Partner/customer-facing facilitator certification materials.

## v1 vs v2 Scope

### v1 (implemented)

- Lab 1 [SQL]: Medallion architecture via SQL Editor with 3 `.dbquery.ipynb` notebooks and step-by-step guide.
- Lab 2 [SQL]: Exploratory queries, CREATE VIEW, saved queries, Databricks Assistant deep-dive, optional Lakeflow Designer.
- Lab 3 [SQL]: Scheduled queries, SQL Alerts, CSV upload with sample file, actual-vs-target views, Databricks One.
- Sample artifact: `labs/artifacts/SQL_Store_Revenue_Targets_2025.csv`.
- Documentation: this takeover guide, repo context baseline, README cross-links.

### v2 (after facilitator validation)

- Advanced alert and monitoring patterns.
- Dedicated Lakeflow Designer hands-on lab.
- Modular content packaging for GenAI assembly and reuse.
- Partner enablement materials and facilitator certification guide.

## Facilitator Decision Points

Use this for workshop routing:

1. **SQL/BI-first audience** with low Spark familiarity → Run full SQL track (Labs 1–3 [SQL]) then shared Labs 2–4.
2. **Data engineering audience** needing pipeline context → Keep SDP path as primary (Lab 1 SDP), skip [SQL] labs.
3. **Mixed audience** → Split into two groups at Lab 1; reconverge at Lab 2 (Metric Views).
4. **Time-limited (< 2 hours)** → Lab 1 [SQL] + Lab 3 [SQL] Steps 3–4 (CSV upload + Databricks One) + Lab 3 (Dashboard consumption demo).
5. **Full day** → Complete SQL track then shared Labs 2–4 for full coverage.

## Learner Flow (SQL Analyst Optional Path)

1. Complete **Lab 0** — environment and data setup.
2. Complete **Lab 1 [SQL]** — build medallion architecture with SQL (bronze/silver/gold).
3. Complete **Lab 2 [SQL]** — explore gold data, create views, use the Databricks Assistant.
4. Complete **Lab 3 [SQL]** — schedule queries, create alerts, upload CSV, explore Databricks One.
5. Continue with **Lab 2** — Data Modelling (Metric Views).
6. Continue with **Lab 3** — Dashboard Creation (AI/BI Dashboards).
7. Continue with **Lab 4** — BI Meets AI (Genie).

## Reusable Assets Inventory

### SQL Lab Assets
- `labs/Lab 1 - [SQL] Data Integration and Transformation.md` — step-by-step guide
- `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Bronze Tables.dbquery.ipynb`
- `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Silver Tables.dbquery.ipynb`
- `labs/Lab 1 - [SQL] Data Integration and Transformation/Create Gold Tables.dbquery.ipynb`
- `labs/Lab 2 - [SQL] SQL Analyst Essentials.md` — views, queries, Assistant exercises
- `labs/Lab 3 - [SQL] Monitoring and Self-Service.md` — alerts, schedules, CSV upload, Databricks One
- `labs/artifacts/SQL_Store_Revenue_Targets_2025.csv` — sample CSV for upload exercise

### Documentation
- `docs/sql_analyst_lab_takeover.md` — this document (takeover guide, gap analysis, facilitator routing)
- `docs/repo_context_baseline.md` — architecture map and change-surface matrix

### Shared Labs (reused for downstream outcomes)
- `labs/Lab 2 - Data Modelling.md`
- `labs/Lab 3 - Dashboard Creation.md`
- `labs/Lab 4 - BI Meets AI.md`

## Views Created by the SQL Track

The SQL Analyst track creates the following views in the `gold` schema:

| View | Created In | Purpose |
|------|-----------|---------|
| `vw_monthly_revenue_summary` | Lab 2 [SQL] | Monthly revenue, cost, profit by store |
| `vw_product_performance` | Lab 2 [SQL] | Product ranking by revenue |
| `vw_actual_vs_target` | Lab 3 [SQL] | Actual vs. budget comparison with status |
