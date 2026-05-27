# Databricks Demo Flow  
*A guided walkthrough for Data Engineering, Metric Views, AI/BI, and Genie Spaces*

## 1. Data Engineering with Spark Declarative Pipelines
- Explain the value of declarative pipelines (Delta Live Tables / Lakehouse pipelines).
- Show how transformations, expectations, and lineage work.
- Highlight automation, reliability, and governance with Unity Catalog integration.

## 2. Creating a Metric View

### 2.1 Concept
- A **Metric View** is a **YAML file** defining business semantics.
- It centralizes KPI logic directly in the Lakehouse.
- It is **client-agnostic** and reusable in SQL, Python, BI tools, AI/BI Dashboards, Agents, and APIs.
- Today supported via YAML; *UI editor is in staging*.

### 2.2 Comparison to Traditional BI
- Power BI / Tableau typically embed metrics in reports → inconsistent & duplicated.
- Databricks Metric Views → governed, consistent, versioned semantics.

### 2.3 Show the Metric View Definition
- Open the YAML file.
- Walk through model, measures, dimensions, filters, and formats.

## 3. Build Visual Analytics with an Agent (AI/BI)

### 3.1 Initial Visual
- Create visual with the Agent with a suggested query
- Remove this visual
- Add chart for **Total Gross Revenue (USD)** over time.

### 3.2 Filters & Formatting
- Add **global date filter** → last 10 years.
- Tooltip: **Total Profit USD**.
- Add labels
- Remove decimals from number formatting.

### 3.3 Store Exploration
- Color / group by **Store Name**.
- Add store filter.
- Enable click-through exploration.

### 3.4 Additional Visuals
- Add **counter visual** for total profit.
- Add image: https://i.ibb.co/cVGnntT/Screenshot-2025-10-16-at-22-04-25.png
- Add **Pie Chart**: Online vs Offline Sales.
- Demo **cross-filtering**.
- Add **line chart**: Total cost by product category.
- Mention that AI functionalities for forecasting are integrated

## 4. New Page – “Focus Store”
- Duplicate header.
- Add **Heatmap Chart** by day of week and product
- Switch back to the **Sales Report** and demonstrate the Drill Through Feature
- Show **Full Screen** mode of the **Heatmap Chart**
- Commit changes to **Git**.
- Explain **Publish** options.
- Show **Download PDF**
- Open shared version.

## 5. Enable Genie Space in the Dashboard
- Active the Genie Space feature in the settings

## 6. Create a Genie Space

Example questions:
- Which products do we offer in the online shop?
- Which products generated the most profit?
- “The Decaf Specialty Blend 250g had a 99% discount in 2021. Deduct it.”
- Show gross revenue of Decaf Specialty Blend over years.

### Parameterized Query Example

```sql
SELECT measure(total_gross_revenue_usd),
       year
FROM sunny_bay_roastery.gold.fact_coffee_sales
WHERE year = :year_param
GROUP BY ALL
ORDER BY 1 ASC;
```
