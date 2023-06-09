WITH dim_sales_order_line_indicator__generate AS (
  SELECT
    'Undersupply Backordered' AS is_undersupply_backordered

  UNION ALL
  
  SELECT
    'Not Undersupply Backordered' AS is_undersupply_backordered

  UNION ALL

  SELECT
    'Undefined' AS is_undersupply_backordered

  UNION ALL

  SELECT
    'Invalid' AS is_undersupply_backordered
)

SELECT
  FARM_FINGERPRINT(
    CONCAT(
      coalesce(dim_sales_order_line_indicator.is_undersupply_backordered, 'Invalid')
      , ','
      , coalesce(dim_package_type.package_type_key, -1)
    )
  ) AS sales_order_line_indicator_key
  , dim_sales_order_line_indicator.is_undersupply_backordered
  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_sales_order_line_indicator__generate AS dim_sales_order_line_indicator

CROSS JOIN {{ ref('dim_package_type') }} AS dim_package_type