# ☕ Lab 2 – Data Modelling: Bring Context and Business Semantics to your data

## 🎯 Learning Objectives
By the end of this lab, you will:
- Understand how [Databricks Metric Views](https://learn.microsoft.com/azure/databricks/metric-views/) will allow you to add business semantics using relationships and calculations to your data
- Create a metric view with
    - relationships to our tables to allow implicit joining of tables.
    - dimensions and measures with attributes and common calcutions
    - formating instructions and synomyns
- Publish the metric view to make it available in Unity Catalog to make it accessable by subsequent features and tools such as Databricks Dashboards.

## Introduction

**What Are Metric Views?**

Metric views are reusable semantic models in Databricks that define business logic for KPIs, calculations, joins, and dimensions in a standardized way.

They allow consistent reporting, simplify complex SQL logic, and centralize metric definitions for dashboards, notebooks, and BI tools.​

**Why Use Metric Views?**

- Ensure “one version of the truth” by standardizing metrics and calculations organization-wide.

- Enable flexible exploration of metrics across any dimension (e.g., sales by product, region, or time) without rebuilding SQL queries.

- Simplify maintenance—updates to metrics or logic are immediately available across all downstream reports and tools.

- Add business context via synonyms, comments, and formatting for user-friendly analytics.​


## Instructions

1. Navigate to the gold schema using the Catalog Explorer and create a new Metric View by selecting it after clicking the New Button. Name it ``sm_fact_coffee_sales``. ![alt text](CreateMetricView.png)

2. Review [provided YAML](./Artifacts/metric_view.yaml) template for reference. This structure includes source, joins, detailed dimensions, measures with formulas, business-friendly display names, and formatting.

3. Define your source: Identify the fact table ("sunny_bay_roastery.gold.fact_coffee_sales"). Make sure you have the name captured correctly. It serves as our base table and is typically the center of our star/snowflake schema.

4. Add joins: Specify dimension tables (customer, product, store, date) and join keys to enrich the fact table.

5. Describe dimensions: List business attributes (e.g., product name, store city, customer loyalty segment) to slice and filter your data.

6. Define measures: Include KPIs (e.g., total orders, gross revenue, profit) using aggregation and calculation logic.

7. (Optional) Use formatting and synonyms: Enhance readability and accessibility for business users.


You can now publish to Unity Catalog by saving the YAML. This makes the metric view discoverable and available to teams and tools, including Databricks Dashboards and downstream analytics, provided they have access enherited from the schema. 



## What Happens Next?
After publishing, users will be able to directly query business metrics without writing SQL joins or recalculating KPIs.

Any updates to the metric view automatically propagate, keeping analytics up to date and reliable throughout the organization.​

This approach empowers business users with governed, understandable, and reusable metrics in Databricks, transforming complex raw data into actionable business insights.