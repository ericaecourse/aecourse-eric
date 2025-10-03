{{ config(schema="intermediate", tags=["intermediate"]) }}

with adspend as (
    select * from {{ ref('stg_adspend') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    date,
    campaign_id as campaign,
    sum(cost_usd) as spend,
    sum(clicks) as clicks,
    sum(impressions) as impressions,
    count(distinct orders.order_id) as total_orders,
    sum(orders.order_total_usd) as total_order_revenue
from adspend
left join orders
    on adspend.date = orders.order_date
    and adspend.campaign_id = orders.utm_campaign
{{ dbt_utils.group_by(2) }}

