WITH dim_state_province__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, dim_state_province__rename_recast AS (
  SELECT
    CAST(state_province_id AS INT) AS state_province_key
    , CAST(state_province_name AS STRING) AS state_province_name
    , CAST(sales_territory AS STRING) AS sales_territory
  FROM dim_state_province__source
)

, dim_state_province__handle_null AS (
  SELECT
    state_province_key
    , state_province_name
    , coalesce(sales_territory, 'Undefined') AS sales_territory
  FROM dim_state_province__rename_recast
)

, dim_state_province__add_undefined_record AS (
  SELECT
    state_province_key
    , state_province_name
    , sales_territory
  FROM dim_state_province__handle_null

  UNION ALL

  SELECT
  0 AS state_province_key
  , 'Undefined' AS state_province_name
  , 'Undefined' AS sales_territory

  UNION ALL

  SELECT
  -1 AS state_province_key
  , 'Invalid' AS state_province_name
  , 'Invalid' AS sales_territory
)

SELECT
    dim_state_province.state_province_key
    , dim_state_province.state_province_name
    , dim_state_province.sales_territory
FROM dim_state_province__add_undefined_record AS dim_state_province
ORDER BY dim_state_province.state_province_key