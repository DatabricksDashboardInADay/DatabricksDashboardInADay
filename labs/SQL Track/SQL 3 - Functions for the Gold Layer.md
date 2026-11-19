# 🛠️ SQL 3 – Functions for the Gold Layer

## 🎯 Learning Objectives
By the end of this lab, you will:
- Flatten nested data (arrays) into rows with **EXPLODE**
- Keep only the right row per group with **QUALIFY** – e.g. the latest version of a customer
- Replace missing values with **COALESCE**
- Derive categories and flags with **CASE** and **IFF**
- Translate source-system codes into readable values with **DECODE**
- Pick "the value belonging to the maximum" with **MAX_BY**
- Calculate several conditional aggregates in one pass with **FILTER (WHERE …)**
- Calculate with dates using **DATEADD** and **DATEDIFF**
- Put everything together and **build a complete gold dimension** – and prove it is correct

## Introduction

**Why This Lab?**

Source systems rarely deliver data the way a report needs it. Orders arrive as nested JSON, customers exist in several versions, fields are empty, and statuses are stored as one-letter codes. The functions in this lab are the toolbox to fix all of that – they are used in almost every silver and gold table of a lakehouse, including the Sunny Bay pipeline.

At the end of the lab you will rebuild the gold table `dim_customer` from the silver layer yourself and compare your result with the table produced by the pipeline.

## Instructions

Before you start, please verify:
- You have completed **SQL 1** and **SQL 2** (Lab 0 created the `sql_sandbox` schema for the tables you build).
- Your SQL warehouse is running and the SQL Editor is open.

1. Set the default catalog again (replace it if you used a different catalog in Lab 0):

```sql
USE CATALOG sunny_bay_roastery;
```

**Step 1: Flatten Arrays with EXPLODE**

The web shop delivers every order as **one JSON document** – the products of the order are stored in an **array** called `items`. Reports need one row per product. `EXPLODE(array)` turns every element of an array into its own row; the other columns are repeated. `POSEXPLODE` additionally returns the position of the element (starting at 0).

1. Run the tiny example:

```sql
SELECT order_id, EXPLODE(items) AS item
FROM VALUES
    ('A-1', ARRAY('Latte', 'Croissant')),
    ('A-2', ARRAY('Espresso'))
    AS t(order_id, items);
```

2. Look at the raw web orders. `payment` and `shipping` are **structs** (you access their fields with a dot, e.g. `shipping.city`), `items` is an **array of structs**:

```sql
SELECT order_id, status, payment.method AS payment_method, shipping.city AS ship_to_city, items
FROM bronze.web_orders
LIMIT 10;
```

3. Explode the items into one row per product, with a line number:

```sql
WITH web_items AS (
    SELECT order_id, POSEXPLODE(items) AS (item_pos, item)
    FROM bronze.web_orders
)
SELECT
    order_id,
    item_pos + 1      AS line_no,
    item.sku          AS sku,
    item.qty          AS quantity,
    item.unit_price   AS unit_price_eur
FROM web_items
ORDER BY order_id, line_no
LIMIT 20;
```

> [!NOTE]
> `EXPLODE` drops rows whose array is empty or `NULL`. If you need to keep them, use `EXPLODE_OUTER`. The silver table `sales_order_line` uses `POSEXPLODE` exactly like above – have a look at `bundle/src/transformations/silver/sales_order_line.sql`.

4. 🏋️ **Your turn:** How many **units per SKU** were ordered online? Work directly on the raw JSON in `bronze.web_orders` (ignore cancelled orders) and sort by units.

<details>
<summary>💡 Solution</summary>

```sql
WITH web_items AS (
    SELECT EXPLODE(items) AS item
    FROM bronze.web_orders
    WHERE status <> 'cancelled'
)
SELECT item.sku, SUM(item.qty) AS units
FROM web_items
GROUP BY item.sku
ORDER BY units DESC;
```

</details>

**Step 2: Keep the Right Row per Group with QUALIFY**

`QUALIFY` filters rows based on the result of a **window function** – just like `HAVING` filters on aggregates. The most common use: keep exactly one row per key, e.g. the **latest version** of a record:

```text
QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <newest first>) = 1
```

1. Run the tiny example. Anna has two versions – which one survives?

```sql
SELECT *
FROM VALUES
    (1, 'Anna', 'Hamburg',  DATE'2024-01-10'),
    (1, 'Anna', 'Lüneburg', DATE'2025-06-01'),
    (2, 'Ben',  'Berlin',   DATE'2024-03-05')
    AS t(customer_id, name, city, updated_at)
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
```

2. Remember the CRM history rows from SQL 1? Here is why they are dangerous. Count the loyalty cards once via the **raw** CRM extract and once directly:

```sql
SELECT
    (SELECT COUNT(*) FROM bronze.crm_loyalty_cards)                AS cards,
    (SELECT COUNT(*)
     FROM bronze.crm_customers AS c
     INNER JOIN bronze.crm_loyalty_cards AS lc
         ON c.customer_id = lc.customer_id)                        AS cards_after_join_with_raw_crm;
```

> [!WARNING]
> The second number is too high: every customer with two CRM versions doubles their cards (**join fan-out**). If this happened in a fact table, revenue would be inflated without anybody noticing. Deduplicate **before** you join.

3. Keep only the latest version of every CRM customer:

```sql
SELECT customer_id, first_name, last_name, email, city, updated_at
FROM bronze.crm_customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
ORDER BY customer_id;
```

4. 🏋️ **Your turn:** In SQL 2 you ranked the products per store with a CTE. Now show only the **top 3 products per store** by gross revenue – in a single query with `QUALIFY` (use `gold.fact_coffee_sales`, `gold.dim_store` and `gold.dim_product`).

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    s.store_name,
    p.product_name,
    SUM(f.gross_revenue_eur) AS revenue_eur
FROM gold.fact_coffee_sales AS f
INNER JOIN gold.dim_store   AS s ON f.store_key = s.store_key
INNER JOIN gold.dim_product AS p ON f.product_key = p.product_key
GROUP BY s.store_name, p.product_name
QUALIFY RANK() OVER (PARTITION BY s.store_name ORDER BY SUM(f.gross_revenue_eur) DESC) <= 3
ORDER BY store_name, revenue_eur DESC;
```

`QUALIFY` is evaluated **after** `GROUP BY`, so the window function can even rank aggregated values.

</details>

**Step 3: Replace Missing Values with COALESCE**

`COALESCE(a, b, c, …)` returns the **first value that is not `NULL`**. It is used to fill gaps with a default value or to take a value from the first source that has it.

1. Run the tiny example:

```sql
SELECT
    COALESCE(NULL, NULL, 'third')   AS first_non_null,
    COALESCE(NULL, 0)               AS default_zero,
    COALESCE('Hamburg', 'Unknown')  AS keeps_value;
```

2. In the POS lines, `discount_amount` is empty when no discount was granted. Aggregates simply **ignore** `NULL`s – which changes the meaning of an average:

```sql
SELECT
    AVG(discount_amount)              AS avg_discount_of_discounted_lines,
    AVG(COALESCE(discount_amount, 0)) AS avg_discount_of_all_lines
FROM bronze.pos_transaction_lines;
```

3. Who placed an order? Café orders know the customer through the **loyalty card**, online orders through the **web account** – and for guests we know nothing. `COALESCE` takes the first customer it finds and uses `-1` ("Guest") as the fallback. This is exactly how the gold fact table fills `customer_key`:

```sql
SELECT
    o.order_id,
    o.sales_channel,
    lc.customer_id                                AS customer_via_card,
    wc.customer_id                                AS customer_via_web_account,
    COALESCE(lc.customer_id, wc.customer_id, -1)  AS customer_key
FROM silver.sales_order AS o
LEFT JOIN silver.loyalty_card AS lc
    ON o.loyalty_card_no = lc.card_no
LEFT JOIN silver.customer AS wc
    ON o.web_account_id = wc.web_account_id
LIMIT 50;
```

> [!TIP]
> Replacing a missing dimension key with a fixed "unknown member" (here `-1`) is a gold-layer best practice: every fact row then finds a partner in the dimension, and reports show a clear "Guest" bucket instead of silently losing rows in an inner join.

4. 🏋️ **Your turn:** Online orders have a delivery city (`ship_to_city`), café orders don't. Count the orders per **"order city"**: the delivery city for online orders, otherwise the city of the store (join `silver.store`).

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    COALESCE(o.ship_to_city, s.city) AS order_city,
    COUNT(*)                         AS orders
FROM silver.sales_order AS o
INNER JOIN silver.store AS s
    ON o.store_id = s.store_id
GROUP BY COALESCE(o.ship_to_city, s.city)
ORDER BY orders DESC;
```

</details>

**Step 4: Derive Values with CASE and IFF**

`CASE` evaluates conditions from top to bottom and returns the result of the **first** condition that is true (or the `ELSE` value). `IFF(condition, value_if_true, value_if_false)` is a compact form for exactly two outcomes.

1. Run the tiny example:

```sql
SELECT
    price_eur,
    CASE
        WHEN price_eur < 3  THEN 'cheap'
        WHEN price_eur < 10 THEN 'regular'
        ELSE 'premium'
    END                                  AS price_band,
    IFF(price_eur >= 10, 'yes', 'no')    AS is_premium
FROM VALUES (2.60), (4.20), (32.00) AS t(price_eur);
```

2. Group the orders into **basket size** bands and label the channel with `IFF`:

```sql
WITH order_totals AS (
    SELECT o.order_id, s.is_online, SUM(l.quantity * l.unit_price_eur - l.discount_eur) AS order_value_eur
    FROM silver.sales_order AS o
    INNER JOIN silver.sales_order_line AS l ON o.order_id = l.order_id
    INNER JOIN silver.store AS s ON o.store_id = s.store_id
    GROUP BY o.order_id, s.is_online
)
SELECT
    IFF(is_online, 'Online shop', 'Café') AS channel,
    CASE
        WHEN order_value_eur < 10 THEN '1 - Under €10'
        WHEN order_value_eur < 25 THEN '2 - €10–€25'
        WHEN order_value_eur < 50 THEN '3 - €25–€50'
        ELSE '4 - €50+'
    END                                   AS basket_size,
    COUNT(*)                              AS orders
FROM order_totals
GROUP BY ALL
ORDER BY channel, basket_size;
```

3. 🏋️ **Your turn:** Count the customers in `silver.customer` per **age group**: `Under 30`, `30–49`, `50 and older` and `Unknown` (web-shop-only customers have no birth date). Calculate the age as `YEAR(current_date()) - YEAR(birth_date)`.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    CASE
        WHEN birth_date IS NULL                              THEN 'Unknown'
        WHEN YEAR(current_date()) - YEAR(birth_date) < 30    THEN 'Under 30'
        WHEN YEAR(current_date()) - YEAR(birth_date) < 50    THEN '30–49'
        ELSE '50 and older'
    END      AS age_group,
    COUNT(*) AS customers
FROM silver.customer
GROUP BY 1
ORDER BY 1;
```

Note that the `NULL` check comes **first**: a comparison with `NULL` is never true, so without it those customers would land in the `ELSE` branch.

</details>

**Step 5: Translate Codes with DECODE**

Source systems love short codes: `B`, `G`, `K`, `M` for payment methods, `S` and `R` for tax classes. `DECODE(expr, search1, result1, search2, result2, …, default)` compares a value with a list of codes and returns the matching result – a compact alternative to a `CASE` with many equality checks.

1. Run the tiny example:

```sql
SELECT
    tier_code,
    DECODE(tier_code, 'B', 'Bronze', 'S', 'Silver', 'G', 'Gold', 'Unknown') AS loyalty_tier
FROM VALUES ('G'), ('B'), ('X') AS t(tier_code);
```

2. Decode the payment codes of the café registers and count the receipts:

```sql
SELECT
    payment_code,
    DECODE(payment_code, 'B', 'Cash', 'G', 'Girocard', 'K', 'Credit Card', 'M', 'Mobile Payment', 'Unknown')
             AS payment_method,
    COUNT(*) AS receipts
FROM bronze.pos_transactions
GROUP BY ALL
ORDER BY receipts DESC;
```

3. The same with `CASE` – compare how much longer it is:

```sql
SELECT
    payment_code,
    CASE payment_code
        WHEN 'B' THEN 'Cash'
        WHEN 'G' THEN 'Girocard'
        WHEN 'K' THEN 'Credit Card'
        WHEN 'M' THEN 'Mobile Payment'
        ELSE 'Unknown'
    END      AS payment_method,
    COUNT(*) AS receipts
FROM bronze.pos_transactions
GROUP BY ALL
ORDER BY receipts DESC;
```

4. 🏋️ **Your turn:** Make the raw ERP product list in `bronze.erp_products` readable: decode `channel_code` (`S` = Café only, `O` = Online only, `B` = Café and online), `status_code` (`A` = Active, `D` = Discontinued) and `tax_class` (`S` = 0.19, `R` = 0.07 VAT rate).

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    sku,
    product_name,
    DECODE(channel_code, 'S', 'Café only', 'O', 'Online only', 'B', 'Café and online', 'Unknown') AS sales_channels,
    DECODE(status_code, 'A', 'Active', 'D', 'Discontinued', 'Unknown')                            AS product_status,
    DECODE(tax_class, 'S', 0.19, 'R', 0.07)                                                       AS vat_rate
FROM bronze.erp_products
ORDER BY sku;
```

Compare your result with `silver.product`, which decodes the same columns.

</details>

**Step 6: Pick the Value at the Maximum with MAX_BY**

`MAX(valid_from)` tells you **when** the latest price started – but not **what** the price is. `MAX_BY(value, ordering)` returns the `value` of the row with the highest `ordering` within a group. `MIN_BY` works the other way round. This avoids a window function or a self-join.

1. Run the tiny example:

```sql
SELECT
    product,
    MAX(valid_from)            AS latest_change,
    MAX_BY(price, valid_from)  AS current_price,
    MIN_BY(price, valid_from)  AS first_price
FROM VALUES
    ('Latte', DATE'2023-01-01', 3.90),
    ('Latte', DATE'2025-08-01', 4.20),
    ('Espresso', DATE'2023-01-01', 2.40)
    AS t(product, valid_from, price)
GROUP BY product;
```

2. The current list price of every product from the raw ERP price history:

```sql
SELECT
    product_id,
    MAX_BY(list_price_eur, valid_from)    AS current_list_price_eur,
    MAX_BY(cost_of_goods_eur, valid_from) AS current_cost_eur
FROM bronze.erp_product_prices
GROUP BY product_id
ORDER BY product_id;
```

3. The **favourite store** of every customer – the store with the most orders:

```sql
WITH store_orders AS (
    SELECT customer_key, store_key, COUNT(DISTINCT order_id) AS orders
    FROM gold.fact_coffee_sales
    WHERE customer_key <> -1
    GROUP BY customer_key, store_key
)
SELECT customer_key, MAX_BY(store_key, orders) AS favourite_store_key
FROM store_orders
GROUP BY customer_key
LIMIT 20;
```

4. 🏋️ **Your turn:** Find the **best day** of every store: the date with the highest gross revenue and the revenue of that day.

<details>
<summary>💡 Solution</summary>

```sql
WITH daily AS (
    SELECT s.store_name, d.date, SUM(f.gross_revenue_eur) AS revenue_eur
    FROM gold.fact_coffee_sales AS f
    INNER JOIN gold.dim_store AS s ON f.store_key = s.store_key
    INNER JOIN gold.dim_date  AS d ON f.date_key = d.date_key
    GROUP BY s.store_name, d.date
)
SELECT
    store_name,
    MAX_BY(date, revenue_eur) AS best_day,
    MAX(revenue_eur)          AS best_day_revenue_eur
FROM daily
GROUP BY store_name
ORDER BY store_name;
```

</details>

**Step 7: Conditional Aggregates with FILTER (WHERE …)**

Often you need the same aggregate for several subsets – e.g. café revenue **and** online revenue in one row. Instead of running several queries, add `FILTER (WHERE …)` to an aggregate function: only the rows that match the condition are aggregated.

1. Run the tiny example:

```sql
SELECT
    COUNT(*)                                    AS all_orders,
    COUNT(*) FILTER (WHERE channel = 'Online')  AS online_orders,
    SUM(amount) FILTER (WHERE channel = 'Café') AS cafe_revenue
FROM VALUES ('Café', 4.20), ('Online', 32.00), ('Café', 2.60) AS t(channel, amount);
```

2. Café vs. online revenue per month – in a single pass over the fact table:

```sql
SELECT
    DATE_TRUNC('MONTH', d.date)                                              AS month,
    SUM(f.gross_revenue_eur) FILTER (WHERE f.sales_channel = 'In-store')    AS cafe_revenue_eur,
    SUM(f.gross_revenue_eur) FILTER (WHERE f.sales_channel = 'Online')      AS online_revenue_eur,
    SUM(f.gross_revenue_eur)                                                 AS total_revenue_eur
FROM gold.fact_coffee_sales AS f
INNER JOIN gold.dim_date AS d ON f.date_key = d.date_key
GROUP BY DATE_TRUNC('MONTH', d.date)
ORDER BY month;
```

3. 🏋️ **Your turn:** For every **café**, count the orders paid with **cash** and the orders paid **cashless** (all other payment methods), and calculate the cash share in percent.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    s.store_name,
    COUNT(*) FILTER (WHERE o.payment_method = 'Cash')                       AS cash_orders,
    COUNT(*) FILTER (WHERE o.payment_method <> 'Cash')                      AS cashless_orders,
    ROUND(100 * COUNT(*) FILTER (WHERE o.payment_method = 'Cash') / COUNT(*), 1) AS cash_share_pct
FROM silver.sales_order AS o
INNER JOIN silver.store AS s
    ON o.store_id = s.store_id
WHERE o.sales_channel = 'In-store'
GROUP BY s.store_name
ORDER BY cash_share_pct DESC;
```

</details>

**Step 8: Calculate with Dates – DATEADD and DATEDIFF**

- `DATEADD(unit, value, expr)` adds (or with a negative value subtracts) days, weeks, months, … to a date or timestamp. It returns a `TIMESTAMP` – wrap it in `CAST(… AS DATE)` if you need a date.
- `DATEDIFF(unit, start, end)` returns the number of units between two dates.

1. Run the tiny example:

```sql
SELECT
    DATEADD(DAY, -1, DATE'2026-01-01')                   AS new_years_eve,
    DATEADD(MONTH, 1, DATE'2026-01-31')                  AS one_month_later,
    CAST(DATEADD(YEAR, 1, DATE'2026-03-15') AS DATE)     AS one_year_later,
    DATEDIFF(DAY, DATE'2026-01-01', DATE'2026-12-24')    AS days_until_christmas_eve;
```

2. The ERP only stores when a price **starts**. The day before the next price starts is the **end** of the current price – this is how `silver.product_price` calculates `valid_to`:

```sql
SELECT
    product_id,
    valid_from,
    CAST(DATEADD(DAY, -1, LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from)) AS DATE) AS valid_to,
    list_price_eur
FROM bronze.erp_product_prices
ORDER BY product_id, valid_from;
```

3. Which customers haven't bought anything for a long time? Calculate the days since the last purchase and flag customers at risk of churning:

```sql
SELECT
    customer_key,
    last_purchase_date,
    DATEDIFF(DAY, last_purchase_date, current_date())                       AS days_since_last_purchase,
    IFF(last_purchase_date < DATEADD(DAY, -90, current_date()), 'yes', 'no') AS churn_risk
FROM gold.dim_customer
WHERE last_purchase_date IS NOT NULL
ORDER BY days_since_last_purchase DESC
LIMIT 50;
```

4. 🏋️ **Your turn:** New loyalty members get a welcome bonus during their **first year** of membership. For every **active** card, calculate the date on which the first year ends and show whether the member is still within the first year today.

<details>
<summary>💡 Solution</summary>

```sql
SELECT
    card_no,
    customer_id,
    enrolled_on,
    CAST(DATEADD(YEAR, 1, enrolled_on) AS DATE)               AS first_year_ends_on,
    current_date() < CAST(DATEADD(YEAR, 1, enrolled_on) AS DATE) AS in_first_year
FROM silver.loyalty_card
WHERE is_active
ORDER BY enrolled_on DESC;
```

</details>

**Step 9: Capstone – Build the Customer Dimension Yourself**

Time to put it all together! The gold table `dim_customer` combines the customer master data with the loyalty program and the purchase behaviour of every customer. You will rebuild it in your `sql_sandbox` schema and then **prove** that your table is identical to the one produced by the pipeline.

Your target – one row per customer with these columns, in this order:

| Column | Logic |
|---|---|
| `customer_key` | `silver.customer.customer_id` |
| `loyalty_segment` | `Prospect` (no orders), `Loyalist` (≥ 100 orders), `Regular` (≥ 10 orders), otherwise `Occasional` |
| `channel_preference` | `Omnichannel` (orders in cafés **and** online), `Online`, `In-store`, otherwise `No purchases yet` |
| `is_home_barista`, `city`, `gender` | from `silver.customer` |
| `loyalty_tier` | tier of the customer's **current** card – the active card first, then the newest; `No membership` if there is none |
| `is_loyalty_member` | `TRUE` if that card is active, otherwise `FALSE` |
| `customer_source` | `silver.customer.source_systems` |
| `registered_at` | from `silver.customer` |
| `first_purchase_date`, `last_purchase_date` | first and last order date |
| `total_orders` | number of distinct orders (0 if none) |
| `favourite_store_key` | store with the most orders |

Plus one extra row for the **unknown member**: `-1, 'Guest', 'Unknown', FALSE, 'Unknown', 'Unknown', 'No membership', FALSE, 'None', NULL, NULL, NULL, 0, NULL`.

1. Build the query step by step. Use one CTE per building block and test each one on its own before you continue:
   - `current_card` – one row per customer from `silver.loyalty_card` (**QUALIFY**)
   - `sales` – orders per customer from `gold.fact_coffee_sales` joined with `gold.dim_date`, with total, café and online orders (**COUNT … FILTER**) and the first and last purchase date (ignore `customer_key = -1`)
   - `favourite_store` – the store with the most orders (**MAX_BY**, see Step 6)
   - the final `SELECT` – `silver.customer` with **LEFT JOINs** to the three CTEs, **CASE** for the segments and **COALESCE** for the defaults
   - the unknown member, appended with **UNION ALL**

2. Save your result as a table:

```text
CREATE OR REPLACE TABLE sql_sandbox.dim_customer AS
WITH ...
SELECT ...
```

<details>
<summary>💡 Solution</summary>

```sql
CREATE OR REPLACE TABLE sql_sandbox.dim_customer AS
WITH current_card AS (
    SELECT customer_id, loyalty_tier, is_active
    FROM silver.loyalty_card
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_active DESC, enrolled_on DESC) = 1
),
sales AS (
    SELECT
        f.customer_key,
        COUNT(DISTINCT f.order_id)                                             AS total_orders,
        COUNT(DISTINCT f.order_id) FILTER (WHERE f.sales_channel = 'In-store') AS store_orders,
        COUNT(DISTINCT f.order_id) FILTER (WHERE f.sales_channel = 'Online')   AS online_orders,
        MIN(d.date)                                                            AS first_purchase_date,
        MAX(d.date)                                                            AS last_purchase_date
    FROM gold.fact_coffee_sales AS f
    INNER JOIN gold.dim_date AS d
        ON f.date_key = d.date_key
    WHERE f.customer_key <> -1
    GROUP BY f.customer_key
),
store_orders AS (
    SELECT customer_key, store_key, COUNT(DISTINCT order_id) AS orders
    FROM gold.fact_coffee_sales
    WHERE customer_key <> -1
    GROUP BY customer_key, store_key
),
favourite_store AS (
    SELECT customer_key, MAX_BY(store_key, orders) AS favourite_store_key
    FROM store_orders
    GROUP BY customer_key
)
SELECT
    c.customer_id                                        AS customer_key,
    CASE
        WHEN COALESCE(s.total_orders, 0) = 0 THEN 'Prospect'
        WHEN s.total_orders >= 100           THEN 'Loyalist'
        WHEN s.total_orders >= 10            THEN 'Regular'
        ELSE 'Occasional'
    END                                                  AS loyalty_segment,
    CASE
        WHEN s.store_orders > 0 AND s.online_orders > 0 THEN 'Omnichannel'
        WHEN s.online_orders > 0                       THEN 'Online'
        WHEN s.store_orders > 0                        THEN 'In-store'
        ELSE 'No purchases yet'
    END                                                  AS channel_preference,
    c.is_home_barista,
    c.city,
    c.gender,
    COALESCE(cc.loyalty_tier, 'No membership')           AS loyalty_tier,
    COALESCE(cc.is_active, FALSE)                        AS is_loyalty_member,
    c.source_systems                                     AS customer_source,
    c.registered_at,
    s.first_purchase_date,
    s.last_purchase_date,
    COALESCE(s.total_orders, 0)                          AS total_orders,
    fs.favourite_store_key
FROM silver.customer AS c
LEFT JOIN current_card AS cc
    ON c.customer_id = cc.customer_id
LEFT JOIN sales AS s
    ON c.customer_id = s.customer_key
LEFT JOIN favourite_store AS fs
    ON c.customer_id = fs.customer_key

UNION ALL

SELECT
    -1, 'Guest', 'Unknown', FALSE, 'Unknown', 'Unknown', 'No membership', FALSE, 'None',
    CAST(NULL AS TIMESTAMP), CAST(NULL AS DATE), CAST(NULL AS DATE), 0, CAST(NULL AS INT);
```

</details>

3. Now prove that your table is correct. Use `MINUS` (Step 2 of SQL 2) in **both directions** – both queries must return **no rows**:

```sql
SELECT * FROM sql_sandbox.dim_customer
MINUS
SELECT * FROM gold.dim_customer;

SELECT * FROM gold.dim_customer
MINUS
SELECT * FROM sql_sandbox.dim_customer;
```

> [!TIP]
> If you get rows back, compare one of the reported customers in both tables (`WHERE customer_key = …`) to find the column that differs. This "reconcile with `MINUS` in both directions" technique is how data engineers test a refactored pipeline against the old one.

4. Finally, open `bundle/src/transformations/gold/dim_customer.sql` in your Git folder. It is the same query you just wrote – now you know every line of a production gold table. 🎓

**Step 10 (Optional): Bonus Capstone – Rebuild the Order Lines from Bronze**

1. 🏋️ **Your turn:** Rebuild `silver.sales_order_line` from the two **raw** sources and save it as `sql_sandbox.sales_order_line` with the columns `order_id`, `line_no`, `product_id`, `sku`, `quantity`, `unit_price_eur` (`DECIMAL(10,2)`), `discount_eur` (`DECIMAL(10,2)`, 0 if missing). You need:
   - the POS lines from `bronze.pos_transaction_lines`
   - the web items from `bronze.web_orders` (**POSEXPLODE**)
   - **UNION ALL** to stack both
   - a **LEFT SEMI JOIN** with `silver.sales_order` to keep only lines of completed orders
   - an **INNER JOIN** with `silver.product` on the SKU to get the `product_id` (and to drop the test item)
   - **COALESCE** for the missing discounts

<details>
<summary>💡 Solution</summary>

```sql
CREATE OR REPLACE TABLE sql_sandbox.sales_order_line AS
WITH pos_lines AS (
    SELECT
        transaction_id                          AS order_id,
        CAST(line_no AS INT)                    AS line_no,
        sku,
        CAST(quantity AS INT)                   AS quantity,
        CAST(unit_price AS DECIMAL(10, 2))      AS unit_price_eur,
        CAST(discount_amount AS DECIMAL(10, 2)) AS discount_eur
    FROM bronze.pos_transaction_lines
),
web_items AS (
    SELECT order_id, POSEXPLODE(items) AS (item_pos, item)
    FROM bronze.web_orders
),
web_lines AS (
    SELECT
        order_id,
        item_pos + 1                            AS line_no,
        item.sku                                AS sku,
        CAST(item.qty AS INT)                   AS quantity,
        CAST(item.unit_price AS DECIMAL(10, 2)) AS unit_price_eur,
        CAST(item.discount AS DECIMAL(10, 2))   AS discount_eur
    FROM web_items
),
all_lines AS (
    SELECT * FROM pos_lines
    UNION ALL
    SELECT * FROM web_lines
)
SELECT
    l.order_id,
    l.line_no,
    p.product_id,
    l.sku,
    l.quantity,
    l.unit_price_eur,
    COALESCE(l.discount_eur, 0) AS discount_eur
FROM all_lines AS l
LEFT SEMI JOIN silver.sales_order AS o
    ON l.order_id = o.order_id
INNER JOIN silver.product AS p
    ON l.sku = p.sku;

SELECT * FROM sql_sandbox.sales_order_line
MINUS
SELECT * FROM silver.sales_order_line;

SELECT * FROM silver.sales_order_line
MINUS
SELECT * FROM sql_sandbox.sales_order_line;
```

</details>

## What Happens Next?

🎉 Congratulations – you have completed the SQL Track! You now know the SQL toolbox that turns raw source-system data into a clean gold layer:

| Problem in the source data | Your tool |
|---|---|
| Rows that must not be counted (voided, cancelled) | `WHERE` |
| Data spread over several systems | `JOIN`s, `UNION ALL` |
| Comparing systems and tables | `FULL OUTER JOIN`, `MINUS`, `INTERSECT` |
| Nested arrays | `EXPLODE` / `POSEXPLODE` |
| Several versions of the same record | `QUALIFY ROW_NUMBER() … = 1` |
| Missing values and unknown keys | `COALESCE`, unknown member `-1` |
| Cryptic codes | `DECODE`, `CASE` |
| "Latest value", "favourite" | `MAX_BY` / `MIN_BY` |
| Several KPIs in one pass | `FILTER (WHERE …)` |
| Validity ranges and time spans | `LEAD` + `DATEADD`, `DATEDIFF` |
| Rankings and comparisons with previous rows | window functions with `PARTITION BY` |
| Hierarchies | self-joins or `WITH RECURSIVE` |

Continue with **Lab 2 – Data Modelling** to turn the gold star schema into a governed **metric view**, or explore the **[SDP] Building the Medallion Pipeline** deep dive to see how the same SQL runs as a declarative pipeline.
