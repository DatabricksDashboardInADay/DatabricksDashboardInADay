-- Reference solution for the Deep Dive [SDP] Building the Medallion Pipeline.
-- In the pipeline every table lives in its own file in bundle/src/transformations/{layer}/.

-- Silver: product_category (business object)
-- The product category tree (root -> category -> subcategory), still in parent/child form.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}product_category
COMMENT 'Product category tree in parent/child form (root -> category -> subcategory).'
AS
SELECT
    CAST(category_id AS INT)        AS category_id,
    CAST(parent_category_id AS INT) AS parent_category_id,
    category_name
FROM bronze.${prefix}erp_product_categories;


-- Silver: product (business object)
-- Product master data with the ERP codes translated into readable attributes (DECODE).
-- The category stays normalized (category_id) – the gold layer flattens the category tree.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}product
COMMENT 'Products from the ERP with decoded channel, status and VAT rate.'
AS
SELECT
    CAST(product_id AS INT)                                        AS product_id,
    sku,
    product_name,
    CAST(category_id AS INT)                                       AS category_id,
    is_beans = 'Y'                                                 AS is_beans,
    channel_code IN ('S', 'B')                                     AS available_in_store,
    channel_code IN ('O', 'B')                                     AS available_online,
    DECODE(status_code, 'A', 'Active', 'D', 'Discontinued', 'Unknown') AS product_status,
    CAST(DECODE(tax_class, 'S', 0.19, 'R', 0.07) AS DECIMAL(4, 2)) AS vat_rate,
    CAST(launched_on AS DATE)                                      AS launched_on
FROM bronze.${prefix}erp_products;


-- Silver: product_price (business object)
-- The ERP only stores the date a price becomes valid (valid_from). The end of the validity
-- (valid_to) is the day before the next price change of the same product (LEAD + DATEADD).
-- DATEADD(unit, value, expr) always returns a TIMESTAMP, so we cast the result back to a DATE.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}product_price
COMMENT 'Product price history with validity ranges (valid_from / valid_to). valid_to is NULL for the current price.'
AS
WITH prices AS (
    SELECT
        CAST(product_id AS INT)                  AS product_id,
        CAST(valid_from AS DATE)                 AS valid_from,
        CAST(list_price_eur AS DECIMAL(10, 2))   AS list_price_eur,
        CAST(cost_of_goods_eur AS DECIMAL(10, 2)) AS cost_of_goods_eur
    FROM bronze.${prefix}erp_product_prices
)
SELECT
    product_id,
    valid_from,
    CAST(DATEADD(DAY, -1, LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from)) AS DATE) AS valid_to,
    list_price_eur,
    cost_of_goods_eur,
    LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) IS NULL          AS is_current
FROM prices;


-- Silver: store (business object)
-- Cafés and the online shop with typed attributes.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}store
COMMENT 'Stores (five cafés in German cities and the online shop).'
AS
SELECT
    CAST(store_id AS INT)               AS store_id,
    store_name,
    store_type,
    city,
    neighborhood_or_channel,
    CAST(opened_on AS DATE)             AS opened_on,
    CAST(is_online AS BOOLEAN)          AS is_online,
    CAST(store_area_sqm AS DOUBLE)      AS store_area_sqm,
    CAST(seating_capacity AS INT)       AS seating_capacity,
    CAST(manager_employee_id AS INT)    AS manager_employee_id,
    country_name,
    country_iso2,
    country_iso3,
    state_province,
    state_iso2,
    county_district,
    CAST(postal_code AS STRING)         AS postal_code,
    street_address,
    CAST(latitude AS DOUBLE)            AS latitude,
    CAST(longitude AS DOUBLE)           AS longitude
FROM bronze.${prefix}erp_stores;


-- Silver: employee (business object)
-- Employees with their manager (manager_id). Together they form the org chart.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}employee
COMMENT 'Employees incl. their manager (org chart). store_id is NULL for head-office staff.'
AS
SELECT
    CAST(employee_id AS INT)            AS employee_id,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS full_name,
    job_title,
    CAST(store_id AS INT)               AS store_id,
    CAST(manager_id AS INT)             AS manager_id,
    CAST(hired_on AS DATE)              AS hired_on
FROM bronze.${prefix}hr_employees;


-- Silver: public_holiday (reference data)
-- German public holidays that apply in all federal states. The cafés are closed on these days.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}public_holiday
COMMENT 'German public holidays valid in all federal states (Bundesländer). Cafés are closed on these days.'
AS
SELECT
    CAST(holiday_date AS DATE) AS holiday_date,
    holiday_name
FROM bronze.${prefix}ref_public_holidays;


-- Silver: loyalty_card (business object)
-- Loyalty program cards with decoded tier names. A customer can own more than one card over
-- time (e.g. a lost card is replaced), so this table is NOT unique per customer.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}loyalty_card
COMMENT 'Loyalty program cards (one row per card). A customer can have several cards over time.'
AS
SELECT
    card_no,
    CAST(customer_id AS INT)                                              AS customer_id,
    DECODE(tier_code, 'B', 'Bronze', 'S', 'Silver', 'G', 'Gold', 'Unknown') AS loyalty_tier,
    CAST(enrolled_on AS DATE)                                             AS enrolled_on,
    CAST(cancelled_on AS DATE)                                            AS cancelled_on,
    cancelled_on IS NULL                                                  AS is_active
FROM bronze.${prefix}crm_loyalty_cards;


-- Silver: customer (business object)
-- One row per customer, integrated from two source systems:
--   * CRM      – the extract contains history rows, so we keep only the latest version (QUALIFY)
--   * Webshop  – customer accounts of the online shop
-- Both systems are matched on the normalized e-mail address (FULL OUTER JOIN), so a person who
-- buys in a café AND online becomes ONE customer.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}customer
COMMENT 'One row per customer, integrated from the CRM (latest version) and the web shop accounts (matched on e-mail).'
AS
WITH crm_latest AS (
    SELECT *
    FROM bronze.${prefix}crm_customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
),
crm AS (
    SELECT
        CAST(customer_id AS INT)                     AS crm_customer_id,
        first_name,
        last_name,
        LOWER(TRIM(email))                           AS email,
        CAST(birth_date AS DATE)                     AS birth_date,
        DECODE(gender_code, 'F', 'Female', 'M', 'Male', 'D', 'Diverse', 'Unknown') AS gender,
        CAST(home_store_id AS INT)                   AS home_store_id,
        city,
        is_home_barista = 'Y'                        AS is_home_barista,
        CAST(created_at AS TIMESTAMP)                AS crm_created_at
    FROM crm_latest
),
web AS (
    SELECT
        CAST(account_id AS INT)                      AS web_account_id,
        LOWER(TRIM(email))                           AS email,
        first_name,
        last_name,
        city,
        CAST(created_at AS TIMESTAMP)                AS web_created_at,
        CAST(newsletter_opt_in AS BOOLEAN)           AS newsletter_opt_in
    FROM bronze.${prefix}web_accounts
)
SELECT
    COALESCE(crm.crm_customer_id, web.web_account_id)    AS customer_id,
    crm.crm_customer_id,
    web.web_account_id,
    COALESCE(crm.first_name, web.first_name)             AS first_name,
    COALESCE(crm.last_name, web.last_name)               AS last_name,
    COALESCE(crm.email, web.email)                       AS email,
    crm.birth_date,
    COALESCE(crm.gender, 'Unknown')                      AS gender,
    crm.home_store_id,
    COALESCE(crm.city, web.city)                         AS city,
    COALESCE(crm.is_home_barista, FALSE)                 AS is_home_barista,
    COALESCE(web.newsletter_opt_in, FALSE)               AS newsletter_opt_in,
    LEAST(crm.crm_created_at, web.web_created_at)        AS registered_at,
    CASE
        WHEN crm.crm_customer_id IS NOT NULL AND web.web_account_id IS NOT NULL THEN 'CRM + Webshop'
        WHEN crm.crm_customer_id IS NOT NULL THEN 'CRM'
        ELSE 'Webshop'
    END                                                  AS source_systems
FROM crm
FULL OUTER JOIN web
    ON crm.email = web.email;


-- Silver: sales_order (business object)
-- One row per completed order from BOTH sales channels:
--   * café receipts from the POS system   (voided receipts are removed)
--   * online orders from the web shop     (cancelled orders are removed)
-- The two sources have different structures and codes, so we map them to one common
-- shape and stack them with UNION ALL.

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}sales_order
COMMENT 'Completed sales orders of all channels (café receipts and online orders), one row per order.'
AS
WITH pos AS (
    SELECT
        transaction_id                                  AS order_id,
        'In-store'                                      AS sales_channel,
        CAST(store_id AS INT)                           AS store_id,
        CAST(transaction_ts AS TIMESTAMP)               AS order_ts,
        loyalty_card_no,
        CAST(NULL AS INT)                               AS web_account_id,
        DECODE(payment_code, 'B', 'Cash', 'G', 'Girocard', 'K', 'Credit Card', 'M', 'Mobile Payment', 'Unknown')
                                                        AS payment_method,
        CAST(cashier_id AS INT)                         AS cashier_id,
        CAST(NULL AS STRING)                            AS voucher_code,
        CAST(NULL AS STRING)                            AS ship_to_city
    FROM bronze.${prefix}pos_transactions
    WHERE status_code = 'C'                 -- C = completed, V = voided
),
web AS (
    SELECT
        order_id,
        'Online'                                        AS sales_channel,
        6                                               AS store_id,      -- the online shop
        CAST(order_ts AS TIMESTAMP)                     AS order_ts,
        CAST(NULL AS STRING)                            AS loyalty_card_no,
        CAST(account_id AS INT)                         AS web_account_id,
        DECODE(payment.method, 'paypal', 'PayPal', 'klarna', 'Klarna', 'credit_card', 'Credit Card', 'Unknown')
                                                        AS payment_method,
        CAST(NULL AS INT)                               AS cashier_id,
        voucher_code,
        shipping.city                                   AS ship_to_city
    FROM bronze.${prefix}web_orders
    WHERE status <> 'cancelled'
),
all_orders AS (
    SELECT * FROM pos
    UNION ALL
    SELECT * FROM web
)
SELECT
    order_id,
    sales_channel,
    store_id,
    order_ts,
    CAST(order_ts AS DATE) AS order_date,
    loyalty_card_no,
    web_account_id,
    payment_method,
    cashier_id,
    voucher_code,
    ship_to_city
FROM all_orders;


-- Silver: sales_order_line (business object)
-- One row per product on an order, from BOTH sales channels:
--   * POS receipt lines are already one row per product
--   * web orders store their items as an ARRAY inside the order -> POSEXPLODE turns every
--     array element into its own row (and gives us the position for the line number)
-- Lines are only kept when the order is a completed order (LEFT SEMI JOIN) and the SKU is a
-- known product (INNER JOIN). Missing discounts are set to 0 (COALESCE).

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}sales_order_line (
    CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW
)
COMMENT 'Order lines of all channels (café receipt lines and exploded web order items), one row per product and order.'
AS
WITH pos_lines AS (
    SELECT
        transaction_id                       AS order_id,
        CAST(line_no AS INT)                 AS line_no,
        sku,
        CAST(quantity AS INT)                AS quantity,
        CAST(unit_price AS DECIMAL(10, 2))   AS unit_price_eur,
        CAST(discount_amount AS DECIMAL(10, 2)) AS discount_eur
    FROM bronze.${prefix}pos_transaction_lines
),
web_items AS (
    SELECT
        order_id,
        POSEXPLODE(items) AS (item_pos, item)
    FROM bronze.${prefix}web_orders
),
web_lines AS (
    SELECT
        order_id,
        item_pos + 1                         AS line_no,
        item.sku                             AS sku,
        CAST(item.qty AS INT)                AS quantity,
        CAST(item.unit_price AS DECIMAL(10, 2)) AS unit_price_eur,
        CAST(item.discount AS DECIMAL(10, 2)) AS discount_eur
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
    COALESCE(l.discount_eur, 0)              AS discount_eur
FROM all_lines AS l
LEFT SEMI JOIN silver.${prefix}sales_order AS o
    ON l.order_id = o.order_id
INNER JOIN silver.${prefix}product AS p
    ON l.sku = p.sku;
