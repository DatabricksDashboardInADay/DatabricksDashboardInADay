# 🔗 SQL 1 – Filtering, Aggregating and Joining Data

## 🎯 Learning Objectives
By the end of this lab, you will:
- Understand the **source systems** of Sunny Bay Roastery and how they flow through **bronze → silver → gold**
- Filter rows with **WHERE** (including the special case of `NULL`)
- Summarize data with **GROUP BY** and filter groups with **HAVING**
- Combine tables with **INNER**, **LEFT**, **FULL OUTER**, **SEMI**, **ANTI** and **CROSS JOIN** – and know when to use which
- Spot typical join pitfalls such as silently dropped rows and messy join keys

## Introduction

**Why This Lab?**

A dashboard is only as good as the gold layer below it, and a good gold layer is mostly built with a handful of SQL patterns: filter the right rows, aggregate them, and join data from different systems together. This SQL Track teaches exactly these patterns on the Sunny Bay Roastery data.

**The Sunny Bay data landscape**

Sunny Bay Roastery runs five cafés (Hamburg, Berlin, München, Köln, Frankfurt am Main) and an online shop. Like every real company, its data lives in **several operational systems**. Each system delivers raw files that land unchanged in the **bronze** layer:

| Source system | Bronze tables | What it contains |
|---|---|---|
| POS (café cash registers) | `pos_transactions`, `pos_transaction_lines` | Café receipts and the products on them |
| Webshop | `web_orders`, `web_accounts` | Online orders (nested JSON) and customer accounts |
| CRM | `crm_customers`, `crm_loyalty_cards` | Customers (with history rows!) and loyalty program cards |
| ERP | `erp_products`, `erp_product_categories`, `erp_product_prices`, `erp_stores` | Product, price and store master data |
| HR | `hr_employees` | Employees and their managers |
| Reference data | `ref_public_holidays` | German public holidays (cafés are closed) |

The **silver** layer cleans these tables and integrates them into **business objects**: `customer`, `loyalty_card`, `product`, `product_category`, `product_price`, `store`, `employee`, `public_holiday`, `sales_order` and `sales_order_line`.

The **gold** layer finally turns the business objects into a **star schema** for reporting: `fact_coffee_sales` with the dimensions `dim_customer`, `dim_product`, `dim_store` and `dim_date`.

> [!NOTE]
> The data covers a rolling window of the **last 24 months** (it is generated when you run Lab 0). Your numbers will therefore differ slightly from your neighbour's if you set up the workshop on different days. The *patterns* in the data are always the same.

This is the first of three SQL labs:

| Lab | Concepts |
|---|---|
| **SQL 1 – Filtering, Aggregating and Joining** (this lab) | WHERE, GROUP BY, INNER / LEFT / FULL OUTER / SEMI / ANTI / CROSS JOIN |
| SQL 2 – Combining and Analyzing Data | UNION / MINUS / INTERSECT, WITH … AS, PARTITION BY, CUBE / ROLLUP, WITH RECURSIVE |
| SQL 3 – Functions for the Gold Layer | EXPLODE, QUALIFY, COALESCE, CASE / IFF, DECODE, MAX_BY, FILTER, DATEADD + capstone |

**How the exercises work**

Every concept follows the same rhythm: a short explanation, a **tiny example** with a few hand-made rows so you can see exactly what happens, an example on the Sunny Bay data, and a **🏋️ Your turn** exercise. Try the exercise yourself first – the solution is hidden behind a **💡 Solution** toggle.

## Instructions

Before you start, please verify:
- You have completed **Lab 0 – Intro** and the setup job has finished (the `bronze`, `silver` and `gold` schemas contain tables).
- A **SQL Warehouse** (e.g. *Serverless Starter Warehouse*) is running.

**Step 1: Open the SQL Editor**

1. In the left sidebar, click **SQL Editor** and select your SQL warehouse in the top-right corner.

2. Set the default catalog. Everything you build in the SQL Track goes into the schema `sql_sandbox`, which Lab 0 already created for you:

```sql
USE CATALOG sunny_bay_roastery;
```

> [!IMPORTANT]
> If you used a different catalog name in Lab 0, replace `sunny_bay_roastery` with your catalog. If you used a **prefix**, all table names start with it (e.g. `silver.jd_customer` instead of `silver.customer`). If you share the catalog with other participants, also put your prefix in front of the tables you create in `sql_sandbox` (e.g. `sql_sandbox.jd_demo_customer`).

3. Take a first look at the silver tables:

```sql
SHOW TABLES IN silver;
```

4. Open one of them to get a feeling for the data:

```sql
SELECT * FROM silver.sales_order LIMIT 10;
```

> [!TIP]
> You can run a single statement by placing the cursor in it and pressing **Ctrl + Enter** (**Cmd + Enter** on Mac). To run everything in the editor press **Ctrl + Shift + Enter**.

**Step 2: Filter Rows with WHERE**

`WHERE` keeps only the rows for which a condition is true. Conditions can be combined with `AND` / `OR` and use operators such as `=`, `<>`, `<`, `BETWEEN`, `IN`, `LIKE` and `IS NULL`.

1. Run the tiny example. Which rows do you expect?

```sql
SELECT *
FROM VALUES
    ('Single Espresso', 2.60, 'Drink'),
    ('Franzbrötchen',   3.20, 'Food'),
    ('House Blend 1kg', 32.00, 'Beans'),
    ('Iced Latte',      4.60, 'Drink')
    AS t(product, price_eur, category)
WHERE price_eur < 5
  AND category IN ('Drink', 'Food');
```

2. The raw POS system marks cancelled receipts with the status code `V` (voided). Let's look at them:

```sql
SELECT transaction_id, store_id, transaction_ts, payment_code, status_code
FROM bronze.pos_transactions
WHERE status_code = 'V'
ORDER BY transaction_ts DESC;
```

> [!NOTE]
> Voided receipts must not be counted as sales. That is why the silver table `sales_order` only keeps receipts with `status_code = 'C'` (completed) – a `WHERE` clause is often the very first cleaning step.

3. A missing value in SQL is `NULL`. In the POS system, `loyalty_card_no` is `NULL` when the guest did not show a loyalty card. Count those receipts:

```sql
SELECT COUNT(*) AS receipts_without_card
FROM bronze.pos_transactions
WHERE loyalty_card_no IS NULL;
```

> [!WARNING]
> `WHERE loyalty_card_no = NULL` returns **no rows at all**, because comparing anything with `NULL` is never true. Always use `IS NULL` / `IS NOT NULL`.

4. 🏋️ **Your turn:** Find all orders in `silver.sales_order` from the **Berlin café** (`store_id = 2`) that were paid with **Girocard** during the **last 7 days**. Show the order id, timestamp and payment method, newest first.

<details>
<summary>💡 Solution</summary>

```sql
SELECT order_id, order_ts, payment_method
FROM silver.sales_order
WHERE store_id = 2
  AND payment_method = 'Girocard'
  AND order_date >= current_date() - INTERVAL 7 DAYS
ORDER BY order_ts DESC;
```

</details>

**Step 3: Summarize with GROUP BY and HAVING**

`GROUP BY` collapses all rows with the same values into **one row per group**. Aggregate functions such as `COUNT`, `SUM`, `AVG`, `MIN` and `MAX` then calculate one value per group. `HAVING` filters the **groups** after aggregation – `WHERE` filters the **rows** before.

1. Run the tiny example:

```sql
SELECT
    category,
    COUNT(*)       AS products,
    AVG(price_eur) AS avg_price_eur,
    MAX(price_eur) AS max_price_eur
FROM VALUES
    ('Single Espresso', 2.60, 'Drink'),
    ('Franzbrötchen',   3.20, 'Food'),
    ('House Blend 1kg', 32.00, 'Beans'),
    ('Iced Latte',      4.60, 'Drink')
    AS t(product, price_eur, category)
GROUP BY category;
```

2. How do customers pay in the cafés and online?

```sql
SELECT
    sales_channel,
    payment_method,
    COUNT(*) AS orders
FROM silver.sales_order
GROUP BY sales_channel, payment_method
ORDER BY sales_channel, orders DESC;
```

> [!TIP]
> Databricks SQL supports `GROUP BY ALL`: it automatically groups by all selected columns that are not aggregated. `GROUP BY ALL` would give the same result as `GROUP BY sales_channel, payment_method` above.

3. Which products are **slow sellers** with less than 5,000 units sold? Here the condition is on an aggregate, so it belongs in `HAVING`:

```sql
SELECT
    sku,
    SUM(quantity)                                            AS units_sold,
    ROUND(SUM(quantity * unit_price_eur - discount_eur), 2) AS gross_revenue_eur
FROM silver.sales_order_line
GROUP BY sku
HAVING SUM(quantity) < 5000
ORDER BY units_sold;
```

4. 🏋️ **Your turn:** The CRM extract `bronze.crm_customers` should contain one row per customer – but does it? Find all `customer_id`s that appear **more than once** and show how many rows each of them has.

<details>
<summary>💡 Solution</summary>

```sql
SELECT customer_id, COUNT(*) AS versions
FROM bronze.crm_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC, customer_id;
```

The CRM delivers **history rows**: when a customer changes their e-mail address or moves, a new version with a newer `updated_at` is added. You will learn how to keep only the latest version with `QUALIFY` in SQL 3 – and in Step 6 you will see what happens if you forget it.

</details>

**Step 4: Prepare Two Tiny Tables for the Join Examples**

A join combines rows of two tables based on a **join condition** (`ON …`). To see clearly what every join type does, we use two tiny tables: three customers and three loyalty cards. Note that *Ben* has no card and card `C-9` belongs to a customer who does not exist.

1. Create the two demo tables in your `sql_sandbox` schema:

```sql
CREATE OR REPLACE TABLE sql_sandbox.demo_customer AS
SELECT * FROM VALUES
    (1, 'Anna'),
    (2, 'Ben'),
    (3, 'Clara')
    AS t(customer_id, name);

CREATE OR REPLACE TABLE sql_sandbox.demo_card AS
SELECT * FROM VALUES
    ('C-1', 1, 'Gold'),
    ('C-3', 3, 'Silver'),
    ('C-9', 9, 'Bronze')
    AS t(card_no, customer_id, tier);
```

2. Keep this overview at hand – it summarizes the joins of this lab:

| Join | Keeps … | Typical question |
|---|---|---|
| `INNER JOIN` | only rows with a match on **both** sides | "Lines **with** their product" |
| `LEFT JOIN` | **all** rows of the left table, matches from the right (or `NULL`) | "All customers, and their card **if they have one**" |
| `FULL OUTER JOIN` | **all** rows of **both** tables | "Compare two systems: who is in A, in B, in both?" |
| `LEFT SEMI JOIN` | left rows that **have** a match (left columns only, no duplicates) | "Products that were **ever** sold online" |
| `LEFT ANTI JOIN` | left rows that have **no** match | "Members who **never** bought anything" |
| `CROSS JOIN` | **every** combination of both tables | "Every store × every day" |

> [!TIP]
> **Two style rules used throughout this workshop:** always write the join type explicitly (`INNER JOIN` instead of a plain `JOIN` – both mean the same, but the explicit form is easier to read next to `LEFT JOIN` & co.), and always introduce a table alias with `AS` (`FROM silver.product AS p`). Both are optional in SQL, but they make queries much easier to read and review.

**Step 5: INNER JOIN – Only Matching Rows**

1. Run the tiny example. Ben (no card) and card `C-9` (no customer) disappear:

```sql
SELECT c.customer_id, c.name, k.card_no, k.tier
FROM sql_sandbox.demo_customer AS c
INNER JOIN sql_sandbox.demo_card AS k
    ON c.customer_id = k.customer_id;
```

2. Order lines only contain a `product_id`. Join the product master to see what was sold:

```sql
SELECT l.order_id, l.line_no, p.product_name, l.quantity, l.unit_price_eur
FROM silver.sales_order_line AS l
INNER JOIN silver.product AS p
    ON l.product_id = p.product_id
LIMIT 20;
```

3. An inner join **silently drops** rows without a match. Compare the number of raw POS lines with the number of lines that find their product via the SKU:

```sql
SELECT
    (SELECT COUNT(*) FROM bronze.pos_transaction_lines)  AS all_pos_lines,
    (SELECT COUNT(*)
     FROM bronze.pos_transaction_lines AS l
     INNER JOIN silver.product AS p
         ON l.sku = p.sku)                                AS lines_with_known_product;
```

> [!NOTE]
> A few lines get lost. They belong to the SKU `SB-9999` – a test item that trainees scanned at the register. In this case dropping them is exactly what we want. But always check your row counts before and after a join: an unexpected difference usually means a data quality problem.

4. 🏋️ **Your turn:** Calculate the **gross revenue per product name** (`quantity * unit_price_eur - discount_eur`) and show the 10 best-selling products.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    p.product_name,
    ROUND(SUM(l.quantity * l.unit_price_eur - l.discount_eur), 2) AS gross_revenue_eur
FROM silver.sales_order_line AS l
INNER JOIN silver.product AS p
    ON l.product_id = p.product_id
GROUP BY p.product_name
ORDER BY gross_revenue_eur DESC
LIMIT 10;
```

</details>

**Step 6: LEFT JOIN – Keep Everything from the Left**

1. Run the tiny example. Ben stays, but his card columns are `NULL`:

```sql
SELECT c.customer_id, c.name, k.card_no, k.tier
FROM sql_sandbox.demo_customer AS c
LEFT JOIN sql_sandbox.demo_card AS k
    ON c.customer_id = k.customer_id;
```

2. Show every customer together with their loyalty card – if they have one:

```sql
SELECT c.customer_id, c.first_name, c.last_name, lc.card_no, lc.loyalty_tier, lc.is_active
FROM silver.customer AS c
LEFT JOIN silver.loyalty_card AS lc
    ON c.customer_id = lc.customer_id
ORDER BY c.customer_id
LIMIT 50;
```

> [!WARNING]
> **Join fan-out:** if the right table has *several* matches, the left row is repeated. Some customers lost their card and got a new one, so they appear twice above. The same happens if you join the raw CRM extract with its history rows – every customer with two versions would suddenly count twice in your revenue. Always know the **grain** (what one row means) of both tables before you join.

3. 🏋️ **Your turn:** How many customers in `silver.customer` have **never had** a loyalty card? Use a `LEFT JOIN` and look for the `NULL`s.

<details>
<summary>💡 Solution</summary>

```sql
SELECT COUNT(*) AS customers_without_card
FROM silver.customer AS c
LEFT JOIN silver.loyalty_card AS lc
    ON c.customer_id = lc.customer_id
WHERE lc.card_no IS NULL;
```

This "LEFT JOIN + IS NULL" pattern is the classic way to find rows **without** a match. In Step 9 you will meet a shorter way to write it: the anti join.

</details>

**Step 7: FULL OUTER JOIN – Compare Two Systems**

1. Run the tiny example. You get Anna, Ben (without card), Clara **and** the orphaned card `C-9` (without customer):

```sql
SELECT c.customer_id, c.name, k.card_no, k.customer_id AS card_customer_id
FROM sql_sandbox.demo_customer AS c
FULL OUTER JOIN sql_sandbox.demo_card AS k
    ON c.customer_id = k.customer_id;
```

2. People can be customers in the cafés (CRM), in the online shop (Webshop) – or both. The only thing the two systems have in common is the **e-mail address**. A full outer join shows all three groups:

```sql
SELECT
    CASE
        WHEN crm.customer_id IS NULL THEN 'Webshop only'
        WHEN web.account_id IS NULL  THEN 'CRM only'
        ELSE 'Both systems'
    END      AS found_in,
    COUNT(*) AS row_count
FROM bronze.crm_customers AS crm
FULL OUTER JOIN bronze.web_accounts AS web
    ON LOWER(TRIM(crm.email)) = web.email
GROUP BY 1
ORDER BY 1;
```

3. 🏋️ **Your turn:** Why do we compare `LOWER(TRIM(crm.email))` instead of simply `crm.email`? Count the matching rows (`INNER JOIN`) once **with** and once **without** `LOWER(TRIM(...))`. Then look at some CRM e-mails that are not already lower case.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    (SELECT COUNT(*)
     FROM bronze.crm_customers AS crm
     INNER JOIN bronze.web_accounts AS web ON crm.email = web.email)              AS raw_matches,
    (SELECT COUNT(*)
     FROM bronze.crm_customers AS crm
     INNER JOIN bronze.web_accounts AS web ON LOWER(TRIM(crm.email)) = web.email) AS normalized_matches;

SELECT customer_id, email
FROM bronze.crm_customers
WHERE email <> LOWER(TRIM(email))
LIMIT 20;
```

The CRM stores e-mails exactly as typed by the café staff – with capital letters and leading or trailing blanks. The web shop stores them in lower case. **Join keys must be normalized** before they can match. The silver table `silver.customer` does exactly this (and also removes the CRM history rows) before it combines both systems with a full outer join.

</details>

**Step 8: LEFT SEMI JOIN – "Has at Least One Match"**

A semi join keeps the rows of the left table that have **at least one** match in the right table. Unlike an inner join it returns only the left columns and **never duplicates** a row, no matter how many matches there are. In standard SQL you write the same with `WHERE EXISTS (…)`.

1. Run the tiny example. Only Anna and Clara remain – without any card columns:

```sql
SELECT *
FROM sql_sandbox.demo_customer AS c
LEFT SEMI JOIN sql_sandbox.demo_card AS k
    ON c.customer_id = k.customer_id;
```

2. Which products were sold **online** at least once?

```sql
SELECT p.product_id, p.product_name
FROM silver.product AS p
LEFT SEMI JOIN (
    SELECT l.product_id
    FROM silver.sales_order_line AS l
    INNER JOIN silver.sales_order AS o
        ON l.order_id = o.order_id
    WHERE o.sales_channel = 'Online'
) AS online_lines
    ON p.product_id = online_lines.product_id
ORDER BY p.product_id;
```

3. The same question written with `EXISTS`:

```sql
SELECT p.product_id, p.product_name
FROM silver.product AS p
WHERE EXISTS (
    SELECT 1
    FROM silver.sales_order_line AS l
    INNER JOIN silver.sales_order AS o
        ON l.order_id = o.order_id
    WHERE o.sales_channel = 'Online'
      AND l.product_id = p.product_id
)
ORDER BY p.product_id;
```

> [!TIP]
> Try replacing `LEFT SEMI JOIN` with `INNER JOIN` in query 2 (and select `p.*`). You get every product thousands of times – once per online order line. Use a semi join whenever you only want to **filter** by the existence of related rows.

4. 🏋️ **Your turn:** Which loyalty cards were used for at least one café purchase during the **last 30 days**? Show card number, customer and tier.

<details>
<summary>💡 Solution</summary>

```sql
SELECT lc.card_no, lc.customer_id, lc.loyalty_tier
FROM silver.loyalty_card AS lc
LEFT SEMI JOIN silver.sales_order AS o
    ON  lc.card_no = o.loyalty_card_no
    AND o.order_date >= current_date() - INTERVAL 30 DAYS
ORDER BY lc.card_no;
```

</details>

**Step 9: LEFT ANTI JOIN – "Has No Match"**

An anti join is the opposite of a semi join: it keeps the left rows that have **no** match at all. In standard SQL you write it with `WHERE NOT EXISTS (…)` or with the "LEFT JOIN + IS NULL" pattern from Step 6.

1. Run the tiny example. Only Ben remains:

```sql
SELECT *
FROM sql_sandbox.demo_customer AS c
LEFT ANTI JOIN sql_sandbox.demo_card AS k
    ON c.customer_id = k.customer_id;
```

2. The marketing team wants to win back loyalty members who signed up but **never bought anything** with their card:

```sql
SELECT lc.card_no, lc.customer_id, lc.loyalty_tier, lc.enrolled_on
FROM silver.loyalty_card AS lc
LEFT ANTI JOIN silver.sales_order AS o
    ON lc.card_no = o.loyalty_card_no
WHERE lc.is_active
ORDER BY lc.enrolled_on DESC;
```

3. 🏋️ **Your turn:** Which products in the product master have **never been sold**? Write the query once with `LEFT ANTI JOIN` and once with `NOT EXISTS`. Then compare your result with the `has_sales` column of `gold.dim_product`.

<details>
<summary>💡 Solution</summary>

```sql
SELECT p.product_id, p.product_name, p.product_status, p.launched_on
FROM silver.product AS p
LEFT ANTI JOIN silver.sales_order_line AS l
    ON p.product_id = l.product_id;

SELECT p.product_id, p.product_name, p.product_status, p.launched_on
FROM silver.product AS p
WHERE NOT EXISTS (
    SELECT 1 FROM silver.sales_order_line AS l WHERE l.product_id = p.product_id
);

SELECT product_key, product_name, has_sales
FROM gold.dim_product
WHERE NOT has_sales;
```

You should find three products: one was discontinued before the sales window and two will be launched soon.

</details>

**Step 10: CROSS JOIN – Every Combination**

A cross join has **no join condition**: it combines every row of the left table with every row of the right table. That sounds dangerous (and it is – 1,000 × 1,000 rows are already one million rows), but it is the right tool to build **complete grids**, e.g. "every store on every day", which you can then compare with the actual data.

1. Run the tiny example – every drink in every size:

```sql
SELECT d.drink, s.size
FROM VALUES ('Latte'), ('Cappuccino') AS d(drink)
CROSS JOIN VALUES ('Small'), ('Medium'), ('Large') AS s(size);
```

2. Build the grid of every store and every day of the sales window and keep the combinations **without any sale** (the anti join from Step 9):

```sql
SELECT s.store_name, d.date, d.day_name, d.holiday_name
FROM silver.store AS s
CROSS JOIN gold.dim_date AS d
LEFT ANTI JOIN silver.sales_order AS o
    ON  o.store_id = s.store_id
    AND o.order_date = d.date
WHERE d.date BETWEEN (SELECT MIN(order_date) FROM silver.sales_order)
                 AND (SELECT MAX(order_date) FROM silver.sales_order)
  AND d.date >= s.opened_on
ORDER BY d.date, s.store_name;
```

> [!NOTE]
> All days without sales are German **public holidays**, on which the cafés are closed. The online shop never appears – it sells every day. Without the cross join you could never see these "missing" rows, because a day without sales simply has no row in the sales table.

3. 🏋️ **Your turn:** How many rows does the grid have **before** the anti join? Count the stores, the days of the sales window and the rows of the cross join, and check that rows = stores × days.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    COUNT(DISTINCT s.store_id) AS stores,
    COUNT(DISTINCT d.date)     AS days,
    COUNT(*)                   AS grid_rows
FROM silver.store AS s
CROSS JOIN gold.dim_date AS d
WHERE d.date BETWEEN (SELECT MIN(order_date) FROM silver.sales_order)
                 AND (SELECT MAX(order_date) FROM silver.sales_order);
```

</details>

**Step 11: Look at a Real Gold Table**

1. Open the definition of the gold fact table in the pipeline: in the Workspace browser navigate to your Git folder → `bundle/src/transformations/gold/fact_coffee_sales.sql`. You will recognize almost everything from this lab:
   - `INNER JOIN` of order lines, orders and products
   - `LEFT JOIN` to the loyalty card and the web account to find the customer
   - a `LEFT JOIN` with a **range condition** (`BETWEEN valid_from AND valid_to`) to pick the price that was valid on the order date

> [!TIP]
> The range join is a very common gold-layer pattern for anything that changes over time: prices, exchange rates, tax rates or the sales region of a customer. The join condition does not have to be an equality!

## What Happens Next?

🎉 Well done! You can now filter, aggregate and – most importantly – join data from different source systems, and you know the classic pitfalls: dropped rows, fan-out and messy join keys.

In **SQL 2 – Combining and Analyzing Data** you will stack result sets with `UNION`, `MINUS` and `INTERSECT`, structure long queries with `WITH`, analyze data with window functions (`PARTITION BY`), build subtotals with `ROLLUP` and `CUBE`, and walk through hierarchies with `WITH RECURSIVE`.
