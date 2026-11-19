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
