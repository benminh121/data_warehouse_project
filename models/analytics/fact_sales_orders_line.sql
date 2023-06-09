-- Fact table coming from Sales Order process
WITH fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_recast AS (
    SELECT 
    CAST(order_line_id AS INT) AS sales_order_line_key
    , CAST(order_id AS INT) AS sales_order_key
    , CAST(stock_item_id AS INT) AS product_key
    , CAST(package_type_id AS INT) AS package_type_key
    , CAST(quantity AS INT) AS quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(tax_rate AS NUMERIC) AS tax_rate
    , CAST(picking_completed_when AS DATE) AS picking_completed_when
    FROM fact_sales_order_line__source
)

, fact_sales_order_line__handle_null AS (
    SELECT
    sales_order_line_key
    , coalesce(sales_order_key, 0) AS sales_order_key
    , coalesce(product_key, 0) AS product_key
    , coalesce(package_type_key, 0) AS package_type_key
    , quantity
    , unit_price
    , tax_rate
    , picking_completed_when
    FROM fact_sales_order_line__rename_recast
)

, fact_sales_order_line__calculated_measure AS (
    SELECT 
    *
    , quantity * unit_price AS gross_amount
    , unit_price * quantity * tax_rate / 100 AS tax_amount
    , (quantity * unit_price) - (unit_price * quantity * tax_rate / 100) AS net_amount

    FROM fact_sales_order_line__handle_null
)

SELECT
    fact_order_line.sales_order_line_key
    , fact_order_line.sales_order_key
    , fact_order_line.product_key
    , coalesce(fact_order.customer_key, -1) AS customer_key
    , coalesce(fact_order.picked_by_person_key, -1) AS picked_by_person_key
    , coalesce(fact_order.salesperson_person_key, -1) AS salesperson_person_key
    , coalesce(fact_order.contact_person_key, -1) AS contact_person_key
    , coalesce(fact_order.customer_purchase_order_number, 'Invalid') AS customer_purchase_order_number
    , fact_order_line.quantity
    , fact_order_line.unit_price
    , fact_order_line.tax_rate
    , fact_order_line.gross_amount
    , fact_order_line.tax_amount
    , fact_order_line.net_amount
    , fact_order.order_date
    , fact_order.expected_delivery_date
    , fact_order_line.picking_completed_when
    , fact_order.order_picking_completed_when
FROM fact_sales_order_line__calculated_measure AS fact_order_line

LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_order
    ON fact_order_line.sales_order_key = fact_order.sales_order_key