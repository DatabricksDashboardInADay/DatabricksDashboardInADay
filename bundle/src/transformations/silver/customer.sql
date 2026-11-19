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
