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
