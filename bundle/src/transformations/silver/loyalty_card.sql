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
