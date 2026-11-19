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
