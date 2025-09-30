{{ config(schema="staging", tags=["staging"]) }}

with raw_adspend as (
    select * from {{ ref('raw_ad_spend_batch1') }}
),

channel_mapping as (
    select * from {{ ref('raw_seed_channel_map') }}
)

select
    date,
    trim(channel) as channel,
    channel_mapping.canonical_channel,
    channel_mapping.paid_flag,
    trim(campaign_id) as campaign_id,
    impressions,
    clicks,
    {{ parse_money('cost_text') }} as cost_usd
from raw_adspend
left join channel_mapping
    on trim(raw_adspend.channel) = trim(channel_mapping.raw_channel)
