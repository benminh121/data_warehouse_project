select
    cast(order_line_id as integer) as sales_order_line_key,
    cast(stock_item_id as integer) as product_key,
    cast(quantity as integer) as quantity,
    cast(unit_price as numeric) as unit_price,
    cast(quantity as integer) * cast(unit_price as numeric) as gross_amount
from `vit-lam-data.wide_world_importers.sales__order_lines`
