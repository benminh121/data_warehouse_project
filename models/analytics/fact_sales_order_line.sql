WITH fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_recast AS (
    SELECT 
        CAST(order_line_id AS INT) AS sales_order_line_key
        , CAST(order_id AS INT) AS sales_order_key
        , CAST(stock_item_id AS INT) AS product_key
        , CAST(quantity AS INT) AS quantity
        , CAST(unit_price AS NUMERIC) AS unit_price
    FROM fact_sales_order_line__source
)

, fact_sales_order_line__calculated_measure AS (
    SELECT 
        *
        , quantity * unit_price AS gross_amount
    FROM fact_sales_order_line__rename_recast
)

SELECT 
    fact_line.sales_order_line_key
    , fact_line.sales_order_key
    , fact_line.product_key
    , coalesce(fact_header.customer_key, -1) as customer_key
    , coalesce(fact_header.picked_by_person_key, -1) as picked_by_person_key
    , fact_line.quantity
    , fact_line.unit_price
    , fact_line.gross_amount
FROM fact_sales_order_line__calculated_measure AS fact_line
LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_header
    ON fact_line.sales_order_key = fact_header.sales_order_key
