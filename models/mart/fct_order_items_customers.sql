{{ config(schema="mart", tags=["mart"]) }}

select
    order_id,
    product_sku,
    order_ts_utc,
    email,
    full_name
from {{ ref('int_order_items_enriched') }}