{{ config(schema="staging", tags=["staging"]) }}

with raw_order_items as (
    select * from {{ ref('raw_order_items_batch1' )}}
)

select
    order_id,
    product_sku,
    COALESCE(qty_text,0) as qty,
    {{ parse_money('unit_price_text') }} as unit_price,
    COALESCE(qty_text,0) * {{ parse_money('unit_price_text') }} as line_revenue
from raw_order_items