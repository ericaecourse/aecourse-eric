{{ config(schema="staging", materialized="table", unique_id="unique_id", tags=["staging"]) }}

with raw_order_items as (
    select * from {{ ref('raw_order_items_batch1' )}}
    union all
    select * from {{ ref('raw_order_items_batch2') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'order_id',
        'product_sku'
    ]) }} as unique_id,
    order_id,
    product_sku,
    COALESCE(qty_text,0) as qty,
    {{ parse_money('unit_price_text') }} as unit_price,
    COALESCE(qty_text,0) * {{ parse_money('unit_price_text') }} as line_revenue
from raw_order_items
{% if is_incremental() %}
  where unique_id not in (select unique_id from {{ this }})
{% endif %}