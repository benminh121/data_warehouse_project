WITH dim_city__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename_recast AS (
  SELECT
    CAST(city_id AS INT) AS city_key
    , CAST(city_name AS STRING) AS city_name
    , CAST(state_province_id AS INT) AS state_province_key
  FROM dim_city__source
)

, dim_city__handle_null AS (
  SELECT
    city_key
    , city_name
    , coalesce(state_province_key, 0) AS state_province_key
  FROM dim_city__rename_recast
)

, dim_city__add_undefined_record AS (
  SELECT
    city_key
    , city_name
    , state_province_key
  FROM dim_city__handle_null

  UNION ALL

  SELECT
    0 AS city_key
    , 'Undefined' AS city_name
    , 0 AS state_province_key

  UNION ALL

  SELECT
    -1 AS city_key
    , 'Invalid' AS city_name
    , -1 AS state_province_key
)

  SELECT
    dim_city.city_key
    , dim_city.city_name
    , dim_city.state_province_key
    , coalesce(dim_state_province.state_province_name, 'Invalid') AS state_province_name
    , coalesce(dim_state_province.sales_territory, 'Invalid') AS sales_territory
  FROM dim_city__add_undefined_record AS dim_city
  LEFT JOIN {{ ref('stg_dim_state_province') }} AS dim_state_province
    ON dim_city.state_province_key = dim_state_province.state_province_key