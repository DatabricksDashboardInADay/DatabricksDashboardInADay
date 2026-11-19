-- Reference solution for the Deep Dive [SDP] Building the Medallion Pipeline.
-- In the pipeline every table lives in its own file in bundle/src/transformations/{layer}/.

-- Gold: dim_date
-- A complete calendar (one row per day, full calendar years) generated with SEQUENCE + EXPLODE,
-- enriched with the German public holidays (LEFT JOIN: most days are not a holiday).

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_date
COMMENT 'Calendar dimension: one row per day covering all years with sales, incl. German public holidays.'
AS
WITH bounds AS (
    SELECT
        MAKE_DATE(YEAR(MIN(order_date)), 1, 1)   AS first_day,
        MAKE_DATE(YEAR(MAX(order_date)), 12, 31) AS last_day
    FROM silver.${prefix}sales_order
),
calendar AS (
    SELECT EXPLODE(SEQUENCE(first_day, last_day, INTERVAL 1 DAY)) AS date
    FROM bounds
)
SELECT
    CAST(DATE_FORMAT(c.date, 'yyyyMMdd') AS INT)  AS date_key,
    c.date,
    YEAR(c.date)                                  AS year,
    MONTH(c.date)                                 AS month,
    DAY(c.date)                                   AS day,
    WEEKOFYEAR(c.date)                            AS calendar_week,
    WEEKDAY(c.date) + 1                           AS day_of_week,   -- 1 = Monday ... 7 = Sunday
    DATE_FORMAT(c.date, 'E')                      AS day_name,
    WEEKDAY(c.date) >= 5                          AS is_weekend,
    CASE
        WHEN MONTH(c.date) IN (12, 1, 2) THEN 'winter'
        WHEN MONTH(c.date) IN (3, 4, 5)  THEN 'spring'
        WHEN MONTH(c.date) IN (6, 7, 8)  THEN 'summer'
        ELSE 'autumn'
    END                                           AS season,
    h.holiday_date IS NOT NULL                    AS is_public_holiday,
    h.holiday_name
FROM calendar AS c
LEFT JOIN silver.${prefix}public_holiday AS h
    ON c.date = h.holiday_date;


-- Gold: dim_product
-- Flattens the normalized product data into one wide row per product:
--   * category tree: the product points to a subcategory, the subcategory to its category
--     -> two joins on the SAME table (self-join) give us both levels
--   * current price: the price row with the latest valid_from (MAX_BY)
--   * has_sales: has the product ever been sold? (LEFT JOIN on the distinct sold products)

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_product
COMMENT 'Product dimension with flattened category hierarchy and current list price / cost (EUR).'
AS
WITH current_price AS (
    SELECT
        product_id,
        MAX_BY(list_price_eur, valid_from)    AS list_price_eur,
        MAX_BY(cost_of_goods_eur, valid_from) AS cost_of_goods_eur
    FROM silver.${prefix}product_price
    GROUP BY product_id
),
sold_products AS (
    SELECT DISTINCT product_id
    FROM silver.${prefix}sales_order_line
)
SELECT
    p.product_id                  AS product_key,
    p.sku,
    p.product_name,
    cat.category_name             AS product_category,
    sub.category_name             AS product_subcategory,
    p.is_beans,
    p.available_in_store,
    p.available_online,
    p.product_status,
    p.vat_rate,
    p.launched_on,
    cp.list_price_eur,
    cp.cost_of_goods_eur,
    s.product_id IS NOT NULL      AS has_sales
FROM silver.${prefix}product AS p
INNER JOIN silver.${prefix}product_category AS sub
    ON p.category_id = sub.category_id
INNER JOIN silver.${prefix}product_category AS cat
    ON sub.parent_category_id = cat.category_id
LEFT JOIN current_price AS cp
    ON p.product_id = cp.product_id
LEFT JOIN sold_products AS s
    ON p.product_id = s.product_id;


-- Gold: dim_store
-- Stores enriched with data from the HR system: the name of the store manager and the
-- number of employees (LEFT JOINs, so a store without HR data would still be kept).

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_store
COMMENT 'Store dimension: five cafés in German cities and the online shop, incl. manager and head count.'
AS
WITH staff AS (
    SELECT store_id, COUNT(*) AS num_employees
    FROM silver.${prefix}employee
    WHERE store_id IS NOT NULL
    GROUP BY store_id
)
SELECT
    s.store_id                         AS store_key,
    s.store_name,
    s.store_type,
    s.city,
    s.neighborhood_or_channel,
    s.is_online,
    s.opened_on,
    s.store_area_sqm,
    s.seating_capacity,
    COALESCE(st.num_employees, 0)      AS num_employees,
    m.full_name                        AS store_manager,
    s.country_name,
    s.country_iso2,
    s.country_iso3,
    s.state_province,
    s.state_iso2,
    s.county_district,
    s.postal_code,
    s.latitude,
    s.longitude
FROM silver.${prefix}store AS s
LEFT JOIN silver.${prefix}employee AS m
    ON s.manager_employee_id = m.employee_id
LEFT JOIN staff AS st
    ON s.store_id = st.store_id;


-- Gold: fact_coffee_sales
-- One row per order line. Brings together lines, orders, products, customers and prices:
--   * customer: café orders are linked through the loyalty card, online orders through the
--     web account. Orders without a known customer get the "Guest" member -1 (COALESCE).
--   * cost of goods: taken from the price row that was valid on the order date (range join).
--   * VAT: depends on the product (19 % standard, 7 % reduced for coffee beans).

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}fact_coffee_sales
COMMENT 'Sales fact: one row per order line (café receipts and online orders), amounts in EUR.'
CLUSTER BY (store_key, date_key)
AS
WITH web_customer AS (
    SELECT web_account_id, customer_id
    FROM silver.${prefix}customer
    WHERE web_account_id IS NOT NULL
),
lines AS (
    SELECT
        o.order_date,
        o.store_id,
        l.product_id,
        COALESCE(lc.customer_id, wc.customer_id, -1)              AS customer_key,
        o.order_id,
        l.line_no,
        o.sales_channel,
        o.payment_method,
        l.quantity,
        l.discount_eur,
        p.vat_rate,
        pp.cost_of_goods_eur                                      AS unit_cost_eur,
        ROUND(l.quantity * l.unit_price_eur - l.discount_eur, 2) AS gross_revenue_eur
    FROM silver.${prefix}sales_order_line AS l
    INNER JOIN silver.${prefix}sales_order AS o
        ON l.order_id = o.order_id
    INNER JOIN silver.${prefix}product AS p
        ON l.product_id = p.product_id
    LEFT JOIN silver.${prefix}loyalty_card AS lc
        ON o.loyalty_card_no = lc.card_no
    LEFT JOIN web_customer AS wc
        ON o.web_account_id = wc.web_account_id
    LEFT JOIN silver.${prefix}product_price AS pp
        ON  l.product_id = pp.product_id
        AND o.order_date BETWEEN pp.valid_from AND COALESCE(pp.valid_to, DATE'9999-12-31')
)
SELECT
    CAST(DATE_FORMAT(order_date, 'yyyyMMdd') AS INT)              AS date_key,
    store_id                                                      AS store_key,
    product_id                                                    AS product_key,
    customer_key,
    order_id,
    line_no                                                       AS order_line_no,
    sales_channel,
    payment_method,
    quantity                                                      AS quantity_sold,
    gross_revenue_eur,
    ROUND(gross_revenue_eur / (1 + vat_rate), 2)                  AS net_revenue_eur,
    gross_revenue_eur - ROUND(gross_revenue_eur / (1 + vat_rate), 2) AS vat_eur,
    discount_eur,
    ROUND(quantity * unit_cost_eur, 2)                            AS cost_of_goods_eur,
    ROUND(gross_revenue_eur / (1 + vat_rate), 2) - ROUND(quantity * unit_cost_eur, 2) AS gross_margin_eur
FROM lines;


-- Gold: dim_customer
-- One row per customer, enriched with behaviour from the sales fact:
--   * the customer's current loyalty card (QUALIFY: latest active card)
--   * order counts per channel in one pass (COUNT ... FILTER (WHERE ...))
--   * favourite store (MAX_BY: the store with the most orders)
--   * segments derived with CASE
-- Plus one extra "unknown member" row (-1, Guest) for orders without a known customer.

CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}dim_customer
AS
WITH current_card AS (
    SELECT customer_id, loyalty_tier, is_active
    FROM silver.${prefix}loyalty_card
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_active DESC, enrolled_on DESC) = 1
),
sales AS (
    SELECT
        f.customer_key,
        COUNT(DISTINCT f.order_id)                                            AS total_orders,
        COUNT(DISTINCT f.order_id) FILTER (WHERE f.sales_channel = 'In-store') AS store_orders,
        COUNT(DISTINCT f.order_id) FILTER (WHERE f.sales_channel = 'Online')   AS online_orders,
        MIN(d.date)                                                           AS first_purchase_date,
        MAX(d.date)                                                           AS last_purchase_date
    FROM gold.${prefix}fact_coffee_sales AS f
    INNER JOIN gold.${prefix}dim_date AS d
        ON f.date_key = d.date_key
    WHERE f.customer_key <> -1
    GROUP BY f.customer_key
),
store_orders AS (
    SELECT customer_key, store_key, COUNT(DISTINCT order_id) AS orders
    FROM gold.${prefix}fact_coffee_sales
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
FROM silver.${prefix}customer AS c
LEFT JOIN current_card AS cc
    ON c.customer_id = cc.customer_id
LEFT JOIN sales AS s
    ON c.customer_id = s.customer_key
LEFT JOIN favourite_store AS fs
    ON c.customer_id = fs.customer_key

UNION ALL

-- the "unknown member": all orders where we do not know the customer
SELECT
    -1, 'Guest', 'Unknown', FALSE, 'Unknown', 'Unknown', 'No membership', FALSE, 'None',
    CAST(NULL AS TIMESTAMP), CAST(NULL AS DATE), CAST(NULL AS DATE), 0, CAST(NULL AS INT);


-- Deep Dive Step 3: aggregated gold table (in the deep dive this lives in its own file gold/total_revenue_by_store.sql)
CREATE OR REFRESH MATERIALIZED VIEW gold.${prefix}total_revenue_by_store AS
SELECT
    store_key AS store_key,
    SUM(gross_revenue_eur) AS total_gross_revenue_eur
FROM gold.${prefix}fact_coffee_sales
GROUP BY store_key;
