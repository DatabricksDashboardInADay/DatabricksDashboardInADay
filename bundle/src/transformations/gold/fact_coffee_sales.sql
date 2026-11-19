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
    ROUND(quantity * unit_cost_eur, 2)                            AS cost_of_goods_eur
FROM lines;
