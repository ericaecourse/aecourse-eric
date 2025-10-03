{{ config(schema="mart", tags=["mart"]) }}

select
    order_id,
    product_sku,
    order_ts_utc,
    oi.email,
    oi.full_name,
    registered_date,
    first_name,
    last_name,
    is_usa
from {{ ref('int_order_items_enriched') }} oi
join {{ ref('dim_customers') }} customers
    on oi.customer_id = customers.customer_id