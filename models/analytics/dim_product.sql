-- 1st layer, select all resource from dim_product
WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

-- 2nd layer, select specific fields and perform CAST
, dim_product__rename_recast AS (
  SELECT 
      CAST(stock_item_id AS INT) AS product_key
    , CAST(stock_item_name AS STRING) AS product_name
    , CAST(brand AS STRING) AS brand_name
    , CAST(supplier_id AS INT) AS supplier_key
    , CAST(is_chiller_stock AS BOOLEAN) AS is_chiller_stock_boolean
  FROM dim_product__source
)

, dim_product__convert_boolean AS (
  SELECT 
    *
    , CASE 
        WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' END
      AS is_chiller_stock
  FROM dim_product__rename_recast
)

, dim_product__add_undefined AS (
  SELECT
    product_key
    , product_name
    , brand_name
    , is_chiller_stock
    , supplier_key
  FROM dim_product__convert_boolean

  UNION ALL
  SELECT
    0 AS product_key
    , 'Undefined' AS product_name
    , 'Undefined' AS brand_name
    , 'Undefined' AS is_chiller_stock
    , 0 AS supplier_key
  
  UNION ALL
  SELECT
    -1 AS product_key
    , 'Invalid' AS product_name
    , 'Invalid' AS brand_name
    , 'Invalid' AS is_chiller_stock
    , -1 AS supplier_key
)

select
  dim_product.product_key
  , dim_product.product_name
  , coalesce(dim_product.brand_name, "Undefined") as brand_name
  , dim_product.supplier_key
  , coalesce(dim_supplier.supplier_name, "Invalid") as supplier_name
  , dim_product.is_chiller_stock
from dim_product__convert_boolean AS dim_product
LEFT JOIN {{ ref('dim_supplier') }} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
