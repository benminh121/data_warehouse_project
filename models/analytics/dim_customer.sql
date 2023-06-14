WITH dim_customer__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_recast AS (
  SELECT
    CAST(customer_id AS INT) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(is_statement_sent AS BOOLEAN) AS is_statement_sent_boolean
    , CAST(is_on_credit_hold AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
    , CAST(payment_days AS INT) AS payment_days
    , CAST(account_opened_date AS DATE) AS account_opened_date
    , CAST(credit_limit AS NUMERIC) AS credit_limit
    , CAST(customer_category_id AS INT) AS customer_category_key
    , CAST(buying_group_id AS INT) AS buying_group_key
    , CAST(bill_to_customer_id AS INT) AS bill_to_customer_key
    , CAST(delivery_method_id AS INT) AS delivery_method_key
    , CAST(delivery_city_id AS INT) AS delivery_city_key
    , CAST(postal_city_id AS INT) AS postal_city_key
    , CAST(primary_contact_person_id AS INT) AS primary_contact_person_key
    , CAST(alternate_contact_person_id AS INT) AS alternate_contact_person_key
  FROM dim_customer__source
)

, dim_customer__convert_boolean AS (
  SELECT *
    , CASE
        WHEN is_statement_sent_boolean IS TRUE THEN 'Sent Statement'
        WHEN is_statement_sent_boolean IS FALSE THEN 'Not Sent Statement'
        WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_statement_sent
    , CASE
        WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
        WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_on_credit_hold
  FROM dim_customer__rename_recast
)

, dim_customer__handle_null AS (
  SELECT
    customer_key
    , customer_name
    , is_statement_sent
    , is_on_credit_hold
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , credit_limit
    , coalesce(customer_category_key, 0) AS customer_category_key
    , coalesce(buying_group_key, 0) AS buying_group_key
    , coalesce(bill_to_customer_key, 0) AS bill_to_customer_key 
    , coalesce(delivery_method_key, 0) AS delivery_method_key
    , coalesce(primary_contact_person_key, 0) AS primary_contact_person_key
    , coalesce(delivery_city_key, 0) AS delivery_city_key
    , coalesce(postal_city_key, 0) AS postal_city_key
    , coalesce(alternate_contact_person_key, 0) AS alternate_contact_person_key
  FROM dim_customer__convert_boolean
)

, dim_customer__add_undefined_record AS (
  SELECT
    customer_key
    , customer_name
    , is_statement_sent
    , is_on_credit_hold
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , credit_limit
    , customer_category_key
    , buying_group_key
    , bill_to_customer_key
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
    , primary_contact_person_key
    , alternate_contact_person_key
  FROM dim_customer__handle_null

  UNION ALL

  SELECT
    0 AS customer_key
    , 'Undefined' AS customer_name
    , 'Undefined' AS is_statement_sent
    , 'Undefined' AS is_on_credit_hold
    , NULL AS standard_discount_percentage
    , NULL AS payment_days
    , NULL AS account_opened_date
    , NULL AS credit_limit
    , 0 AS customer_category_key
    , 0 AS buying_group_key
    , 0 AS bill_to_customer_key
    , 0 AS delivery_method_key
    , 0 AS delivery_city_key
    , 0 as postal_city_key
    , 0 AS primary_contact_person_key
    , 0 AS alternate_contact_person_key

  UNION ALL

  SELECT
    -1 AS customer_key
    , 'Invalid' AS customer_name
    , 'Invalid' AS is_statement
    , 'Invalid' AS is_on_credit
    , NULL AS standard_discount_percentage
    , NULL AS payment_days
    , NULL AS account_opened_date
    , NULL AS credit_limit
    , -1 AS customer_category_key
    , -1 AS buying_group_key
    , -1 AS bill_to_customer_key
    , -1 AS delivery_method_key
    , -1 AS delivery_city_key
    , -1 as postal_city_key
    , -1 AS primary_contact_person
    , -1 AS alternate_contact_pers
)

SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_statement_sent
  , dim_customer.is_on_credit_hold
  , dim_customer.standard_discount_percentage
  , dim_customer.payment_days
  , dim_customer.account_opened_date
  , dim_customer.credit_limit
  , dim_customer.customer_category_key
  , coalesce(dim_customer_category.customer_category_name, 'Invalid') AS customer_category_name
  , dim_customer.buying_group_key
  , coalesce(dim_buying_group.buying_group_name, 'Invalid') AS buying_group_name
  , dim_customer.bill_to_customer_key
  , coalesce(dim_customer_bill_to.customer_name, 'Invalid') AS bill_to_customer_name
  , dim_customer.delivery_method_key
  , coalesce(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
  , dim_customer.delivery_city_key
  , coalesce(dim_city_delivery.city_name, 'Invalid') AS delivery_city_name
  , coalesce(dim_city_delivery.state_province_key, -1) AS delivery_state_province_key
  , coalesce(dim_city_delivery.state_province_name, 'Invalid') AS delivery_state_province_name
  , coalesce(dim_city_postal.state_province_key, -1) AS postal_state_province_key
  , coalesce(dim_city_postal.state_province_name, 'Invalid') AS postal_state_province_name
  , dim_customer.primary_contact_person_key
  , coalesce(dim_person_primary.full_name, 'Invalid') AS primary_contact_full_name
  , dim_customer.alternate_contact_person_key
  , coalesce(dim_person_alternate.full_name, 'Invalid') AS alternate_contact_full_name

FROM dim_customer__add_undefined_record AS dim_customer

LEFT JOIN dim_customer__add_undefined_record AS dim_customer_bill_to
  ON dim_customer.bill_to_customer_key = dim_customer_bill_to.customer_key

LEFT JOIN {{ ref('stg_dim_customer_category') }} AS dim_customer_category
  ON dim_customer.customer_category_key = dim_customer_category.customer_category_key

LEFT JOIN {{ ref('stg_dim_buying_group') }} AS dim_buying_group
  ON dim_customer.buying_group_key = dim_buying_group.buying_group_key

LEFT JOIN {{ ref('dim_person') }} AS dim_person_primary
  ON dim_customer.primary_contact_person_key = dim_person_primary.person_key

LEFT JOIN {{ ref('dim_person') }} AS dim_person_alternate
  ON dim_customer.alternate_contact_person_key = dim_person_alternate.person_key

LEFT JOIN {{ ref('dim_delivery_method') }} AS dim_delivery_method
  ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key

LEFT JOIN {{ ref('dim_city') }} AS dim_city_delivery
  ON dim_customer.delivery_city_key = dim_city_delivery.city_key

LEFT JOIN {{ ref('dim_city') }} AS dim_city_postal
  ON dim_customer.postal_city_key = dim_city_postal.city_key