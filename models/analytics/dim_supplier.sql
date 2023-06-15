WITH dim_supplier__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_recast AS (
  SELECT
    CAST(supplier_id AS INT) AS supplier_key
    , CAST(supplier_name AS STRING) AS supplier_name
    , CAST(supplier_category_id AS INT) AS supplier_category_key
    , CAST(primary_contact_person_id AS INT) AS primary_contact_person_key
    , CAST(alternate_contact_person_id AS INT) AS alternate_contact_person_key
    , CAST(delivery_method_id AS INT) AS delivery_method_key
    , CAST(delivery_city_id AS INT) AS delivery_city_key
  FROM dim_supplier__source
)

, dim_supplier__handle_null AS (
  SELECT
    supplier_key
    , supplier_name
    , coalesce(supplier_category_key, 0) AS supplier_category_key
    , coalesce(primary_contact_person_key, 0) AS primary_contact_person_key
    , coalesce(alternate_contact_person_key, 0) AS alternate_contact_person_key
    , coalesce(delivery_method_key, 0) AS delivery_method_key
    , coalesce(delivery_city_key, 0) AS delivery_city_key
  FROM dim_supplier__rename_recast
)

, dim_supplier__add_undefined_record AS (
  SELECT
    supplier_key
    , supplier_name
    , supplier_category_key
    , primary_contact_person_key
    , alternate_contact_person_key
    , delivery_method_key
    , delivery_city_key
  FROM dim_supplier__handle_null

  UNION ALL
  
  SELECT
    0 AS supplier_key
    , 'Undefined' AS supplier_name
    , 0 AS supplier_category_key
    , 0 AS primary_contact_person_key
    , 0 AS alternate_contact_person_key
    , 0 AS delivery_method_key
    , 0 AS delivery_city_key
  
  UNION ALL

  SELECT
    -1 AS supplier_key
    , 'Invalid' AS supplier_name
    , -1 AS supplier_category_key
    , -1 AS primary_contact_person_key
    , -1 AS alternate_contact_person_key
    , -1 AS delivery_method_key
    , -1 AS delivery_city_key
)

SELECT
  dim_supplier.supplier_key
  , dim_supplier.supplier_name
  , dim_supplier.supplier_category_key
  , coalesce(dim_supplier_category.supplier_category_name, 'Invalid') AS supplier_category_name
  , dim_supplier.primary_contact_person_key
  , coalesce(dim_person_primary_contact.full_name, 'Invalid') AS primary_contact_full_name
  , dim_supplier.alternate_contact_person_key
  , coalesce(dim_person_alternate_contact.full_name, 'Invalid') AS alternate_contact_full_name
  , dim_supplier.delivery_method_key
  , coalesce(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
  , dim_supplier.delivery_city_key
  , coalesce(dim_city.city_name, 'Invalid') AS city_name
  , coalesce(dim_city.state_province_key, -1) AS state_province_key
  , coalesce(dim_city.state_province_name, 'Invalid') AS state_province_name
  , coalesce(dim_city.sales_territory, 'Invalid') AS sales_territory
FROM dim_supplier__add_undefined_record AS dim_supplier

LEFT JOIN {{ ref('stg_dim_supplier_category') }} AS dim_supplier_category
  ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key

LEFT JOIN {{ ref('dim_delivery_method') }} AS dim_delivery_method
  ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key

LEFT JOIN {{ ref('dim_person') }} AS dim_person_primary_contact
  ON dim_supplier.primary_contact_person_key = dim_person_primary_contact.person_key

LEFT JOIN {{ ref('dim_person') }} AS dim_person_alternate_contact
  ON dim_supplier.alternate_contact_person_key = dim_person_alternate_contact.person_key

LEFT JOIN {{ ref('dim_city') }} AS dim_city
  ON dim_supplier.delivery_city_key = dim_city.city_key