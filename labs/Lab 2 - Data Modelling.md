# ☕ Lab 2 – Data Modelling: Bring Context and Business Semantics to your data

## 🎯 Learning Objectives
By the end of this lab, you will:
- Understand how [Databricks Metric Views](https://learn.microsoft.com/azure/databricks/metric-views/) will allow you to add business semantics using relationships and calculations to your data
- Create a metric view with
    - relationships to our tables to allow implicit joining of tables.
    - dimensions and measures with attributes and common calculations
    - formatting instructions and synonyms
- Publish the metric view to make it available in Unity Catalog to make it accessible by subsequent features and tools such as Databricks Dashboards.

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

**Step 1: Create an Empty Metric View**


1. Navigate to the gold schema using the Catalog Explorer and create a new Metric View by selecting it after clicking the New Button.

![alt text](./artifacts/screenshots/MetricView_CreateMetricView.png)

2. Input the name `sm_fact_coffee_sales` for Your Metric View

> [!NOTE]
> The `sm` prefix stands for **semantic model**. We avoid using `mv` as a prefix because it is commonly associated with **materialized views** in developer workflows.

![alt text](./artifacts/screenshots/MetricView_SetName.png)

3. If the YAML editor opened, switch to the UI view. If the UI view opened, you can skip this step.

![alt text](./artifacts/screenshots/MetricView_SwitchToYAML.png)

4. Click on `Select source` to choose the fact table.

5. You now need to navigate to your source table again which is again `sunny_bay_roastery.gold.fact_coffee_sales`. Navigate to it using the Unity Catalog hierarchy. Add it to the Metric View.

![alt text](./artifacts/screenshots/MetricView_UI_SelectSource.png)

**Step 2: Add Joins to Dimension Tables**

> [!NOTE]
> Joins link your fact table to dimension tables, allowing users to slice and filter metrics by attributes like product name, store location, or date — e.g., "show revenue by product category."

1. We will now create our first join. In the overview page, expand the Metric View Canvas by clicking the Arrow button and then click the Join button (2 circles)

![alt text](./artifacts/screenshots/MetricView_UI_OpenJoinDialog.png)

2. Add the `dim_product` table and define the Join Condition using the columns with the `_key` suffix (e.g., `product_key`).

![alt text](./artifacts/screenshots/MetricView_UI_DefineJoin.png)

3. In the following dialog, only select the `Product Name`, `Product Subcategory` and `Product Category`attributes. 

4. See the results of your join-configuration. To get back to the overview page, click the back button. 

![alt text](./artifacts/screenshots/MetricView_UI_JoinResultAndBack.png)

5. Now add the remaining dimension tables using the same approach. Use the following table as a reference:

| Dimension Table | Join Condition | Dimensions to Select |
|---|---|---|
| `dim_date` | `source.date_key = date.date_key` | Date, Day of Week |
| `dim_store` | `source.store_key = store.store_key` | Store Name, Is Online, Latitude, Longitude |

> [!TIP]
> You can also add the joins in the UI.

![alt text](./artifacts/screenshots/MetricView_UI_AddJoin.png)

6. For each dimension you can enter a `Display Name`. This is the bridge between technical column names used by developers (e.g., `product_category`) and human-readable labels for business users (e.g., `Product Category`).

![alt text](./artifacts/screenshots/MetricView_UI_DimensionConf.png)

7. Delete the irrelevant dimension columns such as: `Date Key`, `Txn Seq`, `Product Key`, `Customer Key`, and `Store Key`.

> [!NOTE]
> It is best practice to hide non-relevant and technical columns from business users who consume metric views. This keeps the model clean and easy to navigate.

8. Now create a derived dimension that groups each order into a value band. Open `Dimensions`, click `+ Add`, set the name to `basket_size` (Display Name `Basket Size`), and copy this snippet into the `Expression` field:

```sql
CASE
  WHEN gross_revenue_usd < 10 THEN '1 - Under $10'
  WHEN gross_revenue_usd < 25 THEN '2 - $10–$25'
  WHEN gross_revenue_usd < 50 THEN '3 - $25–$50'
  ELSE '4 - $50+'
END
```

9. Congratulations for creating the basic semantic model of Sunny Bay Roastery. In the next step we are going to integrate measures.

![alt text](./artifacts/screenshots/MetricView_UI_DataModel.png)

**Step 3: Add Measures**

> [!NOTE]
> Measures define the calculations that business users can query — e.g., `SUM(net_revenue_usd)` to get total revenue across any combination of dimensions.

1. Now, we are going to create three measures for `total_net_revenue_usd`, `total_cost_of_goods_usd`, and `total_net_profit_usd`.

2. Open `Measures` and click on `+ Add`. 

![alt text](./artifacts/screenshots/MetricView_UI_AddMeasure.png)

3. Enter the measure name `total_net_revenue_usd` and the expression `SUM(net_revenue_usd)`. You can optionally add more context such as synonyms, comments, and formats. It helps humans to understand the meaning (and agents as well).

![alt text](./artifacts/screenshots/MetricView_UI_TotalNetRevenue.png)

4. Now apply the exact same step for the other two measures:
   - `total_cost_of_goods_usd` with expression `SUM(cost_of_goods_usd)`
   - `total_net_profit_usd` with expression `measure(total_net_revenue_usd) - measure(total_cost_of_goods_usd)`

5. Try creating a new measure using **Genie Code**. Click on `+ Add` and select `Generate with Genie Code`. Describe the measure you want in natural language — for example, "average revenue per order". This is especially helpful for building complex measures without writing the expression from scratch.

![alt text](./artifacts/screenshots/MetricView_UI_GenieCode.png)

**TIP**: you can switch back and forth using the above mentioned switch at the top. To create exactly the same Metric View from above, you can copy the YAML definition over and see/change the results using the GUI. Finally, the Metric View that you defined above will look like this in the UI editor:

![alt text](./artifacts/screenshots/MetricView_UI_Final.png)

6. Test the measure `total_net_revenue_usd` by clicking on the the `Preview` button, and visualizing the measure grouped by `date`.

![alt text](./artifacts/screenshots/MetricView_UI_MeasurePreview.png)

**Step 4: Final Steps**
1. Click on `Save`.

2. You have now published the Metric View to Unity Catalog by saving the YAML. This makes the metric view discoverable and available to teams and tools, including Databricks Dashboards and downstream analytics, provided they have access inherited from the schema. 

3. If you got any errors you couldn't resolve yourself, review the full definition and compare with your results:

```YAML
version: 1.1

source: sunny_bay_roastery.gold.fact_coffee_sales

joins:
  - name: product
    source: sunny_bay_roastery.gold.dim_product
    "on": source.product_key = product.product_key
  - name: date
    source: sunny_bay_roastery.gold.dim_date
    "on": source.date_key = date.date_key
  - name: store
    source: sunny_bay_roastery.gold.dim_store
    "on": source.store_key = store.store_key

dimensions:
  - name: product_name
    expr: product.product_name
    display_name: Product Name
  - name: product_category
    expr: product.product_category
    display_name: Product Category
  - name: product_subcategory
    expr: product.product_subcategory
    display_name: Product Subcategory
  - name: day_of_week
    expr: date.day_of_week
    display_name: Day of Week
  - name: date
    expr: date.date
    display_name: Date
  - name: store_name
    expr: store.store_name
    display_name: Store Name
  - name: store_online
    expr: store.is_online
    display_name: Store Online
  - name: store_latitude
    expr: store.latitude
    display_name: Store Latitude
  - name: store_longitude
    expr: store.longitude
    display_name: Store Longitude
  - name: basket_size
    expr: |
      CASE
        WHEN gross_revenue_usd < 10 THEN '1 - Under $10'
        WHEN gross_revenue_usd < 25 THEN '2 - $10–$25'
        WHEN gross_revenue_usd < 50 THEN '3 - $25–$50'
        ELSE '4 - $50+'
      END
    display_name: Basket Size

measures:
  - name: total_net_revenue_usd
    expr: SUM(net_revenue_usd)
  - name: total_cost_of_goods_usd
    expr: SUM(cost_of_goods_usd)
  - name: total_net_profit_usd
    expr: measure(total_net_revenue_usd) - measure(total_cost_of_goods_usd)
```

## What Happens Next?

You created a simple Metric View and users will be able to directly query business metrics without writing SQL joins or recalculating KPIs.

On purpose, you did not yet use any advanced features such as complex calculations, synonyms, formatting, etc. We encourage you to look into more advanced calculations and modelling capabilities such as:

**Different aggregation functions:**
```YAML 
  - name: max_net_revenue_usd
    expr: MAX(net_revenue_usd)
  - name: avg_cost_of_goods
    expr: AVG(cost_of_goods_usd)
```
    
**Windowing calculations such as rolling averages, previous periods and running totals:**
```YAML 
  - name: total_gross_revenue_usd_previous_day
    expr: measure(`total_gross_revenue_usd`)
    window:
      - order: date
        semiadditive: last
        range: trailing 1 day
```
**Number formatting and synonyms:**
```YAML
  - name: total_gross_revenue_usd
    expr: SUM(`gross_revenue_usd`)
    comment: Total gross revenue in USD before VAT and costs
    display_name: Total Gross Revenue (USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - revenue
      - gross revenue
```

> [!NOTE]
> A richer metric view named `sm_fact_coffee_sales_genie` has already been pre-deployed to your catalog. It includes additional customer dimensions, formatting, synonyms, and windowed measures — providing richer context for AI agents. You can explore it in your catalog at `<catalog>.gold.sm_fact_coffee_sales_genie`.

Once you explored the Metric Views in Unity Catalog, proceed to the next section.
