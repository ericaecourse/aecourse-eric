{{ config(schema="mart", tags=["mart"]) }}

select
    date,
    campaign,
    spend,
    impressions,
    clicks,
    total_orders,
    total_order_revenue,
    round(safe_divide(clicks,impressions),2) *100 as ctr,
    round(safe_divide(spend,impressions),2) as cpi,
    round(safe_divide(spend,clicks),2) as cpc,
    round(safe_divide(spend,total_orders),2) as cpo,
    round(safe_divide(total_order_revenue,spend),2) as roas
from {{ ref('int_marketing_roas') }}