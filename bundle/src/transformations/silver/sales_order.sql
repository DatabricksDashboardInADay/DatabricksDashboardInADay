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
