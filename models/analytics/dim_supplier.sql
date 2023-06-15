WITH dim_supplier__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__add_undefined_record AS (
  SELECT
    CAST(supplier_id AS INT) AS supplier_key
    , CAST(supplier_name AS STRING) AS supplier_name
  FROM dim_supplier__source
)

, dim_supplier__add_undefined_record AS (
  SELECT
    supplier_key
    , supplier_name
  FROM dim_supplier__rename_recast

  UNION ALL
  SELECT
    0 AS supplier_key
    , 'Undefined' AS supplier_name
  
  UNION ALL
  SELECT
    -1 AS supplier_key
    , 'Invalid' AS supplier_name
)

SELECT
  supplier_key
  , supplier_name
FROM dim_supplier__rename_recast