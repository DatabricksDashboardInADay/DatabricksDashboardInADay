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
