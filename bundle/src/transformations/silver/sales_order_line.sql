-- Silver: sales_order_line (business object)
-- One row per product on an order, from BOTH sales channels:
--   * POS receipt lines are already one row per product
--   * web orders store their items as an ARRAY inside the order -> POSEXPLODE turns every
--     array element into its own row (and gives us the position for the line number)
-- Lines are only kept when the order is a completed order (LEFT SEMI JOIN) and the SKU is a
-- known product (INNER JOIN). Missing discounts are set to 0 (COALESCE).

CREATE OR REFRESH MATERIALIZED VIEW silver.${prefix}sales_order_line
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
