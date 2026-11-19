# 📊 SQL 2 – Combining and Analyzing Data

## 🎯 Learning Objectives
By the end of this lab, you will:
- Stack and compare result sets with **UNION ALL**, **UNION**, **MINUS** (EXCEPT) and **INTERSECT**
- Structure long queries into readable building blocks with **WITH … AS** (common table expressions)
- Analyze data with **window functions** and **PARTITION BY** – rankings, running totals and month-over-month comparisons
- Build subtotals and grand totals with **ROLLUP**, **CUBE** and **GROUPING()**
- Walk through hierarchies such as an org chart with **WITH RECURSIVE**

## Introduction

**Why This Lab?**

In SQL 1 you combined tables **side by side** with joins. Many gold-layer tasks need two more skills:

- combining result sets **on top of each other** – e.g. café receipts and online orders into one sales table (`UNION ALL`), or comparing two lists (`MINUS`, `INTERSECT`)
- **analyzing rows in relation to other rows** – e.g. "the previous price of this product", "the first order of this customer", "this month compared to last month". That is what window functions are for.

You will also learn how to keep long queries readable with `WITH`, how to produce report-style subtotals, and how to handle hierarchies of unknown depth.

## Instructions

Before you start, please verify:
- You have completed **SQL 1 – Filtering, Aggregating and Joining** and **Lab 0** created the `sql_sandbox` schema.
- Your SQL warehouse is running and the SQL Editor is open.

1. Set the default catalog again (replace it if you used a different catalog in Lab 0):

```sql
USE CATALOG sunny_bay_roastery;
```

**Step 1: Stack Result Sets with UNION ALL and UNION**

Set operators combine the results of two queries **vertically**. Both queries must return the **same number of columns** with compatible data types; the column names are taken from the first query.

- `UNION ALL` simply appends all rows – fast, and duplicates are kept.
- `UNION` appends and then **removes duplicates** – slower, and only correct if duplicates really are unwanted.

1. Run the tiny example and compare the two results:

```sql
SELECT city FROM VALUES ('Hamburg'), ('Berlin') AS cafe(city)
UNION ALL
SELECT city FROM VALUES ('Berlin'), ('Leipzig') AS online(city);

SELECT city FROM VALUES ('Hamburg'), ('Berlin') AS cafe(city)
UNION
SELECT city FROM VALUES ('Berlin'), ('Leipzig') AS online(city);
```

2. Sunny Bay has two sales systems. Count the raw orders of both systems in one result:

```sql
SELECT 'POS (cafés)' AS source_system, COUNT(*) AS raw_orders
FROM bronze.pos_transactions
UNION ALL
SELECT 'Webshop', COUNT(*)
FROM bronze.web_orders;
```

> [!NOTE]
> The silver table `silver.sales_order` is built exactly like this: the POS receipts and the web orders are first brought into the same shape (same columns, same meaning, same codes) and then stacked with `UNION ALL`. Open `bundle/src/transformations/silver/sales_order.sql` in your Git folder to see it.

3. 🏋️ **Your turn:** Build one list of all **cities** in which Sunny Bay has customers – from the CRM (`bronze.crm_customers.city`) and from the web shop (`bronze.web_accounts.city`). Every city should appear only once, sorted alphabetically.

<details>
<summary>💡 Solution</summary>

```sql
SELECT city FROM bronze.crm_customers
UNION
SELECT city FROM bronze.web_accounts
ORDER BY city;
```

`UNION` (without `ALL`) removes the duplicates. Note that `ORDER BY` at the end sorts the **combined** result.

</details>

**Step 2: Compare Lists with MINUS and INTERSECT**

- `MINUS` (or its synonym `EXCEPT`) returns the rows of the first query that do **not** appear in the second query.
- `INTERSECT` returns the rows that appear in **both** queries.

Both remove duplicates and compare **all** selected columns.

1. Run the tiny example:

```sql
SELECT sku FROM VALUES ('SB-0101'), ('SB-0102'), ('SB-0103') AS master(sku)
MINUS
SELECT sku FROM VALUES ('SB-0102'), ('SB-0103'), ('SB-9999') AS sold(sku);

SELECT sku FROM VALUES ('SB-0101'), ('SB-0102'), ('SB-0103') AS master(sku)
INTERSECT
SELECT sku FROM VALUES ('SB-0102'), ('SB-0103'), ('SB-9999') AS sold(sku);
```

2. Which SKUs from the product master have **never** been sold?

```sql
SELECT sku FROM silver.product
MINUS
SELECT sku FROM silver.sales_order_line;
```

3. And the other way round: which SKUs were scanned at the café registers but are **unknown** in the product master?

```sql
SELECT sku FROM bronze.pos_transaction_lines
EXCEPT
SELECT sku FROM silver.product;
```

> [!TIP]
> `MINUS` / `EXCEPT` in both directions is the quickest **reconciliation** check in data engineering: if `A MINUS B` and `B MINUS A` both return no rows, two tables contain exactly the same rows. You will use this in the capstone of SQL 3.

4. 🏋️ **Your turn:** Which e-mail addresses exist in **both** the CRM (`bronze.crm_customers`) and the web shop (`bronze.web_accounts`)? Remember the lesson from SQL 1: normalize the CRM e-mails with `LOWER(TRIM(...))`. Then count them.

<details>
<summary>💡 Solution</summary>

```sql
SELECT COUNT(*) AS emails_in_both_systems
FROM (
    SELECT LOWER(TRIM(email)) AS email FROM bronze.crm_customers
    INTERSECT
    SELECT email FROM bronze.web_accounts
) AS both_systems;
```

These are the **omnichannel** customers who buy in the cafés *and* online. `silver.customer` merges each of them into a single customer.

</details>

**Step 3: Structure Queries with WITH … AS**

A **common table expression (CTE)** gives a sub-query a name. You define one or more named building blocks after `WITH` and use them like tables in the final query. CTEs make long queries readable from top to bottom, and a block can be reused several times.

1. Run the tiny example:

```sql
WITH prices AS (
    SELECT * FROM VALUES ('Latte', 4.20), ('Espresso', 2.60), ('Flat White', 4.30) AS t(drink, price_eur)
),
expensive AS (
    SELECT * FROM prices WHERE price_eur > 4
)
SELECT COUNT(*) AS expensive_drinks FROM expensive;
```

2. What is the **average order value** in the cafés and online? First sum up every order, then average the order totals per channel:

```sql
WITH order_totals AS (
    SELECT
        order_id,
        SUM(quantity * unit_price_eur - discount_eur) AS order_value_eur
    FROM silver.sales_order_line
    GROUP BY order_id
)
SELECT
    o.sales_channel,
    COUNT(*)                         AS orders,
    ROUND(AVG(t.order_value_eur), 2) AS avg_order_value_eur
FROM order_totals AS t
INNER JOIN silver.sales_order AS o
    ON t.order_id = o.order_id
GROUP BY o.sales_channel;
```

3. 🏋️ **Your turn:** The following query finds the stores whose number of **Saturday** orders is above the average of all stores. It works, but it is hard to read. Rewrite it with two CTEs: `saturday_orders` (orders per store) and `average` (the average of these numbers).

```sql
SELECT store_name, saturday_orders
FROM (
    SELECT s.store_name, COUNT(*) AS saturday_orders
    FROM silver.sales_order AS o
    INNER JOIN silver.store AS s ON o.store_id = s.store_id
    WHERE DAYOFWEEK(o.order_date) = 7
    GROUP BY s.store_name
) AS t
WHERE saturday_orders > (
    SELECT AVG(cnt)
    FROM (
        SELECT COUNT(*) AS cnt
        FROM silver.sales_order
        WHERE DAYOFWEEK(order_date) = 7
        GROUP BY store_id
    ) AS x
);
```

<details>
<summary>💡 Solution</summary>

```sql
WITH saturday_orders AS (
    SELECT s.store_name, COUNT(*) AS saturday_orders
    FROM silver.sales_order AS o
    INNER JOIN silver.store AS s ON o.store_id = s.store_id
    WHERE DAYOFWEEK(o.order_date) = 7      -- 1 = Sunday ... 7 = Saturday
    GROUP BY s.store_name
),
average AS (
    SELECT AVG(saturday_orders) AS avg_saturday_orders
    FROM saturday_orders
)
SELECT so.store_name, so.saturday_orders
FROM saturday_orders AS so
CROSS JOIN average AS a
WHERE so.saturday_orders > a.avg_saturday_orders
ORDER BY so.saturday_orders DESC;
```

The CTE `saturday_orders` is used twice – once for the average and once for the final result. The `CROSS JOIN` with a one-row CTE is a common trick to attach a single value to every row.

</details>

**Step 4: Analyze with Window Functions and PARTITION BY**

`GROUP BY` collapses rows into one row per group. A **window function** calculates a value over a group of rows **but keeps every row**. The `OVER (…)` clause defines the window:

- `PARTITION BY` – which rows belong together (like a `GROUP BY`, but without collapsing)
- `ORDER BY` – the order of the rows inside each partition (needed for rankings, running totals and `LAG` / `LEAD`)

| Function | What it returns |
|---|---|
| `ROW_NUMBER()` | 1, 2, 3, … per partition |
| `RANK()` | rank with gaps for ties (1, 1, 3) |
| `SUM(x) OVER (…)` | a total – or a **running total** if the window has an `ORDER BY` |
| `LAG(x)` / `LEAD(x)` | the value of the **previous** / **next** row |

1. Run the tiny example and look at every column:

```sql
SELECT
    store,
    month,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY store ORDER BY month)  AS month_no,
    SUM(revenue) OVER (PARTITION BY store)                 AS store_total,
    SUM(revenue) OVER (PARTITION BY store ORDER BY month)  AS running_total,
    LAG(revenue) OVER (PARTITION BY store ORDER BY month)  AS previous_month
FROM VALUES
    ('Hamburg', 1, 100), ('Hamburg', 2, 120), ('Hamburg', 3, 90),
    ('Berlin',  1, 80),  ('Berlin',  2, 95)
    AS t(store, month, revenue)
ORDER BY store, month;
```

2. Rank the products **per store** by revenue:

```sql
WITH product_revenue AS (
    SELECT s.store_name, p.product_name, SUM(f.gross_revenue_eur) AS revenue_eur
    FROM gold.fact_coffee_sales AS f
    INNER JOIN gold.dim_store   AS s ON f.store_key = s.store_key
    INNER JOIN gold.dim_product AS p ON f.product_key = p.product_key
    GROUP BY s.store_name, p.product_name
)
SELECT
    store_name,
    product_name,
    revenue_eur,
    RANK() OVER (PARTITION BY store_name ORDER BY revenue_eur DESC) AS rank_in_store
FROM product_revenue
ORDER BY store_name, rank_in_store;
```

3. Compare every month with the **previous month** per store (month-over-month growth):

```sql
WITH monthly AS (
    SELECT
        s.store_name,
        DATE_TRUNC('MONTH', d.date)  AS month,
        SUM(f.gross_revenue_eur)     AS revenue_eur
    FROM gold.fact_coffee_sales AS f
    INNER JOIN gold.dim_store AS s ON f.store_key = s.store_key
    INNER JOIN gold.dim_date  AS d ON f.date_key = d.date_key
    GROUP BY s.store_name, DATE_TRUNC('MONTH', d.date)
)
SELECT
    store_name,
    month,
    revenue_eur,
    LAG(revenue_eur) OVER (PARTITION BY store_name ORDER BY month) AS previous_month_eur,
    ROUND(100 * (revenue_eur / LAG(revenue_eur) OVER (PARTITION BY store_name ORDER BY month) - 1), 1)
                                                                   AS growth_pct
FROM monthly
ORDER BY store_name, month;
```

4. The ERP stores a new row whenever a price changes, but only with a `valid_from` date. With `LEAD` you can see the start date of the **next** price – the day before it is the end of the current price. This is exactly how `silver.product_price` calculates `valid_to`:

```sql
SELECT
    product_id,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_price_from,
    list_price_eur
FROM bronze.erp_product_prices
ORDER BY product_id, valid_from;
```

5. 🏋️ **Your turn:** For every loyalty card, find the **first café order** (the order with the earliest `order_ts`). Number the orders per card with `ROW_NUMBER()` in a CTE and keep only number 1. Show card number, order id and timestamp.

<details>
<summary>💡 Solution</summary>

```sql
WITH numbered_orders AS (
    SELECT
        loyalty_card_no,
        order_id,
        order_ts,
        ROW_NUMBER() OVER (PARTITION BY loyalty_card_no ORDER BY order_ts) AS order_no
    FROM silver.sales_order
    WHERE loyalty_card_no IS NOT NULL
)
SELECT loyalty_card_no, order_id, order_ts
FROM numbered_orders
WHERE order_no = 1
ORDER BY order_ts;
```

You cannot write `WHERE ROW_NUMBER() OVER (…) = 1` directly, because `WHERE` is evaluated **before** window functions. That's why we need the CTE – or the `QUALIFY` clause you will learn in SQL 3.

</details>

**Step 5: Subtotals with ROLLUP, CUBE and GROUPING()**

Reports often need subtotals and a grand total. Instead of writing several queries and stacking them with `UNION ALL`, you can let `GROUP BY` produce them:

- `GROUP BY ROLLUP (a, b)` – groups by `(a, b)`, then subtotals per `a`, then the grand total. Use it for **hierarchies** such as year → month.
- `GROUP BY CUBE (a, b)` – **all** combinations: `(a, b)`, `(a)`, `(b)` and the grand total. Use it for independent dimensions.
- `GROUPING(a)` returns `1` if the row is a subtotal over `a` (and `a` is therefore `NULL`), otherwise `0`.

1. Run the tiny example:

```sql
SELECT
    year,
    month,
    SUM(revenue)    AS revenue,
    GROUPING(month) AS is_year_total
FROM VALUES (2025, 11, 100), (2025, 12, 150), (2026, 1, 120) AS t(year, month, revenue)
GROUP BY ROLLUP (year, month)
ORDER BY year NULLS LAST, month NULLS LAST;
```

2. Revenue per year and month with yearly subtotals and a grand total:

```sql
SELECT
    d.year,
    d.month,
    SUM(f.gross_revenue_eur) AS revenue_eur,
    CASE
        WHEN GROUPING(d.year) = 1  THEN 'Grand total'
        WHEN GROUPING(d.month) = 1 THEN 'Year total'
        ELSE 'Month'
    END AS row_type
FROM gold.fact_coffee_sales AS f
INNER JOIN gold.dim_date AS d ON f.date_key = d.date_key
GROUP BY ROLLUP (d.year, d.month)
ORDER BY d.year NULLS LAST, d.month NULLS LAST;
```

3. Orders by sales channel **and** payment method, with totals in every direction:

```sql
SELECT
    CASE WHEN GROUPING(sales_channel) = 1 THEN 'All channels' ELSE sales_channel END AS channel,
    CASE WHEN GROUPING(payment_method) = 1 THEN 'All methods' ELSE payment_method END AS method,
    COUNT(DISTINCT order_id) AS orders
FROM gold.fact_coffee_sales
GROUP BY CUBE (sales_channel, payment_method)
ORDER BY channel, method;
```

> [!NOTE]
> Subtotals are great for reports and exports. In a **dashboard** you usually don't need them: the metric view and the dashboard calculate totals on the fly.

4. 🏋️ **Your turn:** Show the gross revenue per **store** and **product category** (from `gold.dim_store` and `gold.dim_product`), with a subtotal per store and a grand total. Label the subtotal rows.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    s.store_name,
    p.product_category,
    SUM(f.gross_revenue_eur) AS revenue_eur,
    CASE
        WHEN GROUPING(s.store_name) = 1       THEN 'Grand total'
        WHEN GROUPING(p.product_category) = 1 THEN 'Store total'
        ELSE 'Category'
    END AS row_type
FROM gold.fact_coffee_sales AS f
INNER JOIN gold.dim_store   AS s ON f.store_key = s.store_key
INNER JOIN gold.dim_product AS p ON f.product_key = p.product_key
GROUP BY ROLLUP (s.store_name, p.product_category)
ORDER BY s.store_name NULLS LAST, p.product_category NULLS LAST;
```

</details>

**Step 6: Walk Through Hierarchies with WITH RECURSIVE**

Some data is a **tree**: every employee has a manager, every product category has a parent category. With normal joins you need one join per level – but how many levels are there? A **recursive CTE** walks through a tree of any depth. It always has two parts, combined with `UNION ALL`:

- the **anchor**: the starting rows (e.g. the person without a manager)
- the **recursive part**: joins the source table to the CTE itself to find the next level – and repeats until no new rows are found

> [!NOTE]
> Recursive CTEs need a current SQL warehouse (or Databricks Runtime 17.0 and above). If you get a syntax error, ask your facilitator to check the warehouse channel.

1. Run the tiny example – a mini org chart with three levels:

```sql
WITH RECURSIVE chart (name, level) AS (
    SELECT name, 1
    FROM VALUES ('Benno', NULL), ('Katrin', 'Benno'), ('Jonas', 'Benno'), ('Mila', 'Katrin') AS t(name, manager)
    WHERE manager IS NULL
    UNION ALL
    SELECT t.name, c.level + 1
    FROM VALUES ('Benno', NULL), ('Katrin', 'Benno'), ('Jonas', 'Benno'), ('Mila', 'Katrin') AS t(name, manager)
    INNER JOIN chart AS c ON t.manager = c.name
)
SELECT * FROM chart ORDER BY level, name;
```

2. Build the complete **org chart** of Sunny Bay Roastery from the HR data, with the level and the reporting path of every employee:

```sql
WITH RECURSIVE org_chart (employee_id, full_name, job_title, level, reporting_path) AS (
    -- anchor: the founder has no manager
    SELECT employee_id, full_name, job_title, 1, full_name
    FROM silver.employee
    WHERE manager_id IS NULL

    UNION ALL

    -- recursive part: everybody whose manager is already in the org chart
    SELECT e.employee_id, e.full_name, e.job_title, oc.level + 1, CONCAT(oc.reporting_path, ' > ', e.full_name)
    FROM silver.employee AS e
    INNER JOIN org_chart AS oc
        ON e.manager_id = oc.employee_id
)
SELECT level, job_title, full_name, reporting_path
FROM org_chart
ORDER BY reporting_path;
```

3. 🏋️ **Your turn:** Build the **category path** of every product, e.g. `All Products > Drink > Milk Coffee > Cappuccino`. Start the recursion at the root category (`parent_category_id IS NULL`) of `silver.product_category`, walk down the tree, and finally join the products to their category (`silver.product.category_id`).

<details>
<summary>💡 Solution</summary>

```sql
WITH RECURSIVE category_path (category_id, path, depth) AS (
    SELECT category_id, category_name, 1
    FROM silver.product_category
    WHERE parent_category_id IS NULL

    UNION ALL

    SELECT c.category_id, CONCAT(cp.path, ' > ', c.category_name), cp.depth + 1
    FROM silver.product_category AS c
    INNER JOIN category_path AS cp
        ON c.parent_category_id = cp.category_id
)
SELECT
    p.product_name,
    CONCAT(cp.path, ' > ', p.product_name) AS full_path,
    cp.depth + 1                           AS depth
FROM silver.product AS p
INNER JOIN category_path AS cp
    ON p.category_id = cp.category_id
ORDER BY full_path;
```

> [!TIP]
> Have a look at `bundle/src/transformations/gold/dim_product.sql`: the gold layer flattens the category tree **without** recursion – it joins `product_category` twice (a *self-join*). That works because the tree has a fixed depth of three levels. Use recursion when the depth can vary (like an org chart).

</details>

## What Happens Next?

🎉 Great job! You can now stack and compare result sets, structure your queries with CTEs, analyze rows in context with window functions, produce subtotals and walk through hierarchies.

In **SQL 3 – Functions for the Gold Layer** you will learn the functions that turn messy source data into clean gold tables – `EXPLODE`, `QUALIFY`, `COALESCE`, `CASE`/`IFF`, `DECODE`, `MAX_BY`, `FILTER` and `DATEADD` – and you will finish the SQL Track by building a complete gold dimension yourself.
