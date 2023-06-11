WITH stg_fact_sales_order__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_recast AS (
  SELECT
    CAST(order_id AS INT) AS sales_order_key
    , CAST(customer_purchase_order_number AS STRING) AS customer_purchase_order_number
    , CAST(customer_id AS INT) AS customer_id
    , CAST(picked_by_person_id AS INT) AS picked_by_person_key
    , CAST(salesperson_person_id AS INT) AS salesperson_person_key
    , CAST(contact_person_id AS INT) AS contact_person_key
    , CAST(order_date AS DATE) AS order_date
    , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
    , CAST(picking_completed_when AS DATE) AS order_picking_completed_when
  FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__handle_null AS (
  SELECT
    sales_order_key
    , coalesce(customer_purchase_order_number, 'Undefined') AS customer_purchase_order_number
    , coalesce(customer_id, 0) AS customer_id
    , coalesce(picked_by_person_key, 0) AS picked_by_person_key
    , coalesce(salesperson_person_key, 0) AS salesperson_person_key
    , coalesce(contact_person_key, 0) AS contact_person_key
    , order_date
    , expected_delivery_date
    , order_picking_completed_when
  FROM stg_fact_sales_order__rename_recast
)

, stg_fact_sales_order__add_undefined AS (
  SELECT
    sales_order_key
    , customer_purchase_order_number
    , customer_id
    , picked_by_person_key
    , salesperson_person_key
    , contact_person_key
    , order_date
    , expected_delivery_date
    , order_picking_completed_when
  FROM stg_fact_sales_order__handle_null
  
  UNION ALL

  SELECT
    0 AS sales_order_key
    , 'Undefined' AS customer_purchase_order_number
    , 0 AS customer_id
    , 0 AS picked_by_person_key
    , 0 AS salesperson_person_key
    , 0 AS contact_person_key
    , NULL AS order_date
    , NULL AS expected_delivery_date
    , NULL AS order_picking_completed_when

  UNION ALL

  SELECT
    -1 AS sales_order_key
    , 'Invalid' AS customer_purchase_order_number
    , -1 AS customer_id
    , -1 AS picked_by_person_key
    , -1 AS salesperson_person_key
    , -1 AS contact_person_key
    , NULL AS order_date
    , NULL AS expected_delivery_date
    , NULL AS order_picking_completed_when
)

SELECT
  sales_order_key
  , customer_purchase_order_number
  , customer_id
  , picked_by_person_key
  , salesperson_person_key
  , contact_person_key
  , order_date
  , expected_delivery_date
  , order_picking_completed_when
FROM stg_fact_sales_order__add_undefined