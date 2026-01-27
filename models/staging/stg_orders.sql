{{ config(schema="staging", materialized="incremental", unique_id="unique_id", tags=["staging"]) }}

with raw_orders as (
    select * from {{ ref('raw_orders_batch1') }}
    union all
    select * from {{ ref('raw_orders_batch2') }}
    qualify row_number () over (partition by order_id order by updated_at desc) = 1
),

orders_transformed as (
select
    order_id,
    customer_id,
    {{ try_to_ts('order_ts') }} as order_ts_utc,
    date({{ try_to_ts('order_ts') }}) as order_date,
    lower(trim(status)) as status,
    upper(trim(currency)) as currency,
    {{ parse_money('total_amount_text') }} as total_amount,
    {{ json_text('utm_json','source') }} as utm_source,
    {{ json_text('utm_json','medium') }} as utm_medium,
    {{ json_text('utm_json','campaign') }} as utm_campaign,
    updated_at
from raw_orders
)

, usd_exchange_rates as (
    select * from {{ ref('raw_seed_currency_fx_rates') }}
)

select
    orders_transformed.*,
    ROUND(total_amount * COALESCE(usd_exchange_rates.usd_rate,1.0),2) as order_total_usd
from orders_transformed
left join usd_exchange_rates
    on orders_transformed.order_date = usd_exchange_rates.date
    and orders_transformed.currency = usd_exchange_rates.currency
{% if is_incremental() %}
  where order_ts_utc > (select max(order_ts_utc) from {{ this }})
{% endif %}

