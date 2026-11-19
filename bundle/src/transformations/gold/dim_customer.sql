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
