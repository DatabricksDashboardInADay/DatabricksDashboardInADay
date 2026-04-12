# 🧪 [SQL] Monitoring and Self-Service: Alerts, Schedules & CSV Upload

## 🎯 Learning Objectives
By the end of this lab, you will:
- **Schedule a SQL query** to run automatically on a recurring basis  
- Create a **SQL Alert** that notifies you when a business metric crosses a threshold  
- **Upload a CSV file** to Unity Catalog and query it alongside existing gold data  
- Create a view that compares **actual vs. target** revenue  
- Navigate **Databricks One** to find dashboards, data assets, and Genie from a single experience  

> **Note:** This is a **SQL Analyst** deep dive. If your facilitator has directed you to the shared track, proceed directly to **Lab 2 – Data Modelling (Metric Views)**. Both paths are fully compatible with Labs 2–4.

## Introduction

**Why This Lab?**

In [SQL] SQL Analyst Essentials you explored data and created reusable views. Now it's time to productionise your work:

- **Scheduled queries** automate recurring reports so you don't have to run them manually every day  
- **Alerts** monitor your data and notify you when something needs attention — a revenue drop, a data quality issue, or a missed target  
- **CSV upload** lets you bring in external data (budgets, targets, lookup lists) without waiting for a data engineer  
- **Databricks One** gives you a unified home screen to find everything — dashboards, datasets, queries, and Genie spaces  

This is how analysts move from ad-hoc exploration to reliable, monitored workflows.

## Instructions

Before you start, please verify:
- You have completed **[SQL] SQL Analyst Essentials** and the views `vw_monthly_revenue_summary` and `vw_product_performance` exist in Unity Catalog.
- Your **SQL Warehouse** (Serverless Starter Warehouse or Pro) is running.
- The **SQL Editor** is open and connected to your warehouse.

**Step 1: Schedule a Recurring Query**

Scheduled queries run automatically at set intervals and can notify you when results are ready. You will schedule a monthly revenue summary to run daily.

**1. Create the query to schedule**

In the SQL Editor, run and then **save** the following query with the name `Daily — Monthly Revenue Summary`:

```sql
SELECT
    year,
    month,
    store_name,
    monthly_gross_revenue_usd,
    monthly_profit_usd,
    monthly_orders
FROM sunny_bay_roastery.gold.vw_monthly_revenue_summary
WHERE year = YEAR(CURRENT_DATE())
ORDER BY month DESC, store_name;
```

**2. Add a schedule**

1. With the saved query open, click the **Schedule** button in the top-right toolbar (clock icon).
2. Configure the schedule:
   - **Frequency:** Every 1 day  
   - **Time:** Choose a time (e.g., 08:00)  
   - **SQL Warehouse:** Select your Serverless Starter Warehouse  
3. (Optional) Under **Notifications**, add your email to receive results when the query completes.
4. Click **Save** to activate the schedule.

**3. Verify the schedule**

1. In the sidebar, navigate to **Queries**.
2. Find your `Daily — Monthly Revenue Summary` query.
3. You should see a schedule indicator (clock icon) next to the query name.

**💡 What just happened?**

- The SQL Warehouse will automatically wake up, run this query, and (optionally) email you the results on schedule.
- Scheduled queries are ideal for daily/weekly reports, data freshness checks, and KPI monitoring.
- You can pause or delete the schedule at any time from the query settings.

**Step 2: Create a SQL Alert**

SQL Alerts monitor the result of a query and trigger a notification when a condition is met. You will create an alert that fires when any store's monthly revenue falls below a threshold.

**1. Create the alert query**

Save the following query with the name `Alert — Low Monthly Revenue`:

```sql
SELECT
    store_name,
    year,
    month,
    monthly_gross_revenue_usd
FROM sunny_bay_roastery.gold.vw_monthly_revenue_summary
WHERE year = YEAR(CURRENT_DATE())
  AND month = MONTH(CURRENT_DATE())
  AND monthly_gross_revenue_usd < 50000
ORDER BY monthly_gross_revenue_usd ASC;
```

This query returns rows only when a store's current-month revenue is below $50,000.

**2. Create the alert**

1. In the Databricks sidebar, click **Alerts**.
2. Click **Create Alert**.
3. Configure the alert:
   - **Query:** Select the `Alert — Low Monthly Revenue` query you just saved.
   - **Trigger condition:** Set to **"Rows > 0"** — the alert fires whenever the query returns at least one row (meaning at least one store is below the $50,000 threshold).
   - **Title:** `Low Monthly Store Revenue Alert`
   - **Refresh schedule:** Every 1 day (or match your scheduled query frequency).
4. Under **Notifications**, add your preferred notification destination (email, Slack, or webhook).
5. Click **Create Alert**.

**3. Understand alert states**

| State | Meaning |
|-------|---------|
| **OK** | The query returned 0 rows — all stores are above the threshold |
| **TRIGGERED** | The query returned 1+ rows — at least one store is below $50,000 |
| **UNKNOWN** | The alert has not run yet or the query failed |

**💡 What just happened?**

- You created a data-driven alert that monitors a business KPI without any manual checking.
- When the condition is met, you (and your team) are notified automatically.
- This pattern scales: you can create alerts for data quality (e.g., "row count dropped by 50%"), SLA monitoring (e.g., "pipeline hasn't updated in 24 hours"), or any SQL-expressible condition.

**🔍 Try it yourself:**

Create a second alert that monitors product performance. For example: *"Alert me if any product's total units sold drops below 100 in the current month."* Use the `vw_product_performance` view or write a new query.

**Step 3: Upload a CSV File and Explore Self-Service Data**

One of the most common analyst workflows is bringing in external reference data — budgets, targets, competitor benchmarks, or ad-hoc lookup files — and joining it to existing enterprise data. Databricks makes this simple with **file upload to Unity Catalog**.

**1. Download the sample CSV**

A sample revenue target file has been provided for this exercise:

**`labs/artifacts/[SQL] Monitoring and Self-Service/SQL_Store_Revenue_Targets_2025.csv`**

This file contains monthly revenue targets for each Sunny Bay store in 2025:

| store_key | store_name | target_year | target_monthly_revenue_usd |
|-----------|-----------|-------------|---------------------------|
| 1 | Sunny Bay – Market Street | 2025 | 55000.00 |
| 2 | Sunny Bay – Mission | 2025 | 45000.00 |
| 3 | Sunny Bay – Westfield Mall | 2025 | 35000.00 |
| 4 | Sunny Bay – SoMa Offices | 2025 | 50000.00 |
| 5 | Sunny Bay – Hayes Valley | 2025 | 40000.00 |
| 6 | Sunny Bay Online | 2025 | 70000.00 |

If you are running this lab in a Databricks workspace with access to GitHub, download this file from the repository. Otherwise, your facilitator will provide it.

**2. Upload the CSV to Unity Catalog**

1. In the sidebar, navigate to **Catalog > sunny_bay_roastery > gold**.
2. Click the **Create Table** button and select **Upload file**.
3. Select or drag the `SQL_Store_Revenue_Targets_2025.csv` file.
4. Databricks will preview the data and infer column types. Verify that:
   - `store_key` is detected as **INT**
   - `target_monthly_revenue_usd` is detected as **DOUBLE** or **DECIMAL**
5. Set the table name to `revenue_targets_2025`.
6. Click **Create Table**.

**3. Query the uploaded data**

Confirm the upload by querying the new table:

```sql
SELECT * FROM sunny_bay_roastery.gold.revenue_targets_2025;
```

You should see 6 rows — one target per store.

**4. Join targets to actual revenue**

Now write a query that compares actual monthly revenue to the targets:

```sql
SELECT
    mrs.store_name,
    mrs.year,
    mrs.month,
    mrs.monthly_gross_revenue_usd AS actual_revenue_usd,
    rt.target_monthly_revenue_usd AS target_revenue_usd,
    ROUND(mrs.monthly_gross_revenue_usd - rt.target_monthly_revenue_usd, 2) AS variance_usd,
    ROUND(
      (mrs.monthly_gross_revenue_usd - rt.target_monthly_revenue_usd)
      / rt.target_monthly_revenue_usd * 100
    , 1) AS variance_pct
FROM sunny_bay_roastery.gold.vw_monthly_revenue_summary mrs
JOIN sunny_bay_roastery.gold.revenue_targets_2025 rt
  ON mrs.store_name = rt.store_name
WHERE mrs.year = 2025
ORDER BY mrs.month, variance_pct ASC;
```

**5. Create a view for ongoing use**

Wrap the comparison into a reusable view:

```sql
CREATE OR REPLACE VIEW sunny_bay_roastery.gold.vw_actual_vs_target AS
SELECT
    mrs.store_name,
    mrs.year,
    mrs.month,
    mrs.monthly_gross_revenue_usd AS actual_revenue_usd,
    rt.target_monthly_revenue_usd AS target_revenue_usd,
    ROUND(mrs.monthly_gross_revenue_usd - rt.target_monthly_revenue_usd, 2) AS variance_usd,
    ROUND(
      (mrs.monthly_gross_revenue_usd - rt.target_monthly_revenue_usd)
      / rt.target_monthly_revenue_usd * 100
    , 1) AS variance_pct,
    CASE
      WHEN mrs.monthly_gross_revenue_usd >= rt.target_monthly_revenue_usd THEN 'On Track'
      WHEN mrs.monthly_gross_revenue_usd >= rt.target_monthly_revenue_usd * 0.9 THEN 'At Risk'
      ELSE 'Below Target'
    END AS status
FROM sunny_bay_roastery.gold.vw_monthly_revenue_summary mrs
JOIN sunny_bay_roastery.gold.revenue_targets_2025 rt
  ON mrs.store_name = rt.store_name
  AND mrs.year = rt.target_year;
```

Query the view for 2025 to see which stores are meeting their targets:

```sql
SELECT * FROM sunny_bay_roastery.gold.vw_actual_vs_target
WHERE year = 2025
ORDER BY month, status DESC, variance_pct ASC;
```

**💡 What just happened?**

- You uploaded an external CSV file and it became a fully governed Unity Catalog table in seconds.
- You joined it to enterprise data using standard SQL — no ETL pipeline needed.
- The `vw_actual_vs_target` view is now available to dashboards, Genie, and other analysts.
- This workflow is ideal for budgets, targets, one-off lookup files, and any ad-hoc reference data that doesn't justify a full pipeline.

**🔍 Try it yourself:**

Think of another CSV file that would be useful to upload. For example:
- A list of products that are Fair Trade certified (hint: check `labs/artifacts/Lab 4 - BI Meets AI/Genie_FairTrade.csv`)
- Regional marketing campaign dates and budgets
- Customer satisfaction survey results by store

**Step 4: Discover Databricks One**

**Databricks One** is the unified home experience that brings together dashboards, data assets, queries, Genie spaces, and more in a single searchable interface. As a SQL analyst, this is your starting point for finding and consuming insights.

**1. Open Databricks One**

1. In the Databricks sidebar, click the **Databricks logo** at the top-left (or the **Home** icon, depending on your workspace version).

**2. Explore the home screen**

The Databricks One home screen surfaces:

- **Recent items** — dashboards, queries, and notebooks you've recently viewed or edited.
- **Recommended** — assets shared with you or popular in your workspace.
- **Search** — a universal search bar that finds dashboards, tables, views, queries, and Genie spaces.

**3. Find your work**

1. Use the search bar to search for `sunny_bay` — you should find your gold tables, views, and saved queries.
2. Click on **Dashboards** in the left navigation to see all published dashboards. Look for `[Final] Sunny Bay Roastery - Sales Report` (deployed by Lab 0).
3. Open the dashboard as a **consumer** — notice how it uses the same gold data you built and enriched.

**4. Preview the Genie experience**

1. Click on **Genie** in the left navigation.
2. If a Genie space has already been created, you can ask a question in natural language (e.g., *"What was the total revenue in 2024?"*).
3. If no Genie space exists yet, don't worry — you will create one in **Lab 4**.

**💡 What just happened?**

- Databricks One gives analysts a single pane of glass to find everything: data, reports, AI assistants, and shared queries.
- Everything you built in Labs 1–3 [SQL] is discoverable here — tables, views, scheduled queries, and alerts.
- This is the experience that bridges the SQL workflow (Labs 1–3 [SQL]) to the semantic and visual tools in Labs 2–4 (Metric Views, Dashboards, Genie).

## Final Steps

You have now:

- **Scheduled** a recurring query that runs daily against your revenue view
- **Created an alert** that monitors store revenue and notifies you when it drops below target
- **Uploaded a CSV** and joined it to gold data to compare actual vs. target performance
- **Explored Databricks One** as the unified entry point for all data assets

These are the core productivity features that make Databricks approachable and powerful for SQL analysts — no Spark, no notebooks, no pipelines required.

## Facilitator Guidance

Use this section to adapt this lab for different audiences and time constraints.

| Scenario | Recommendation |
|----------|---------------|
| **Full day, SQL-focused audience** | Run all steps. Budget ~45–60 minutes. |
| **Half day, mixed audience** | Run Steps 1–2 (scheduled queries + alerts), demo Steps 3–4. Budget ~25 minutes. |
| **Time-constrained (< 30 min)** | Demo Steps 2 and 3, skip Step 1 (scheduling). Prioritise the CSV upload as the highest-impact exercise. |
| **Already familiar with SQL alerts** | Skip Step 2, focus on Step 3 (CSV upload) and Step 4 (Databricks One). |

**Decision point:** After completing this lab, SQL analysts should proceed to **Lab 2 (Metric Views)** to learn how business semantics are layered on top of the gold tables they created. The [SQL] track and shared track converge at Lab 2.

## What Happens Next?

You have completed the SQL Analyst–specific labs. Your gold tables, views, scheduled queries, and alerts are all in place.

Now continue with the shared workshop track:
- **Lab 2** – Data Modelling: Build Metric Views to add business semantics to your gold data
- **Lab 3** – Dashboard Creation: Create interactive AI/BI Dashboards using Metric Views
- **Lab 4** – BI Meets AI: Explore Databricks Genie for natural-language analytics

The gold tables you built are identical to those produced by the SDP path, so all shared labs work exactly the same regardless of which Lab 1 path you chose.
