{{ config(schema="staging", materialized="table", unique_id="unique_id", tags=["staging"]) }}

with raw_adspend as (
    select * from {{ ref('raw_ad_spend_batch1') }}
    union all
    select * from {{ ref('raw_ad_spend_batch2') }}
),

raw_spend_agg as (
select
    date,
    channel,
    campaign_id,
    sum({{ parse_money('cost_text') }}) as cost_text,
    sum(impressions) as impressions,
    sum(clicks) as clicks
from raw_adspend
group by 1,2,3
),

channel_mapping as (
    select * from {{ ref('raw_seed_channel_map') }}
),

final as (
select
    {{ dbt_utils.generate_surrogate_key([
        'date',
        'channel',
        'campaign_id',
        'paid_flag'
    ]) }} as unique_id,
    date,
    trim(channel) as channel,
    channel_mapping.canonical_channel,
    channel_mapping.paid_flag,
    trim(campaign_id) as campaign_id,
    impressions,
    clicks,
    cost_text as cost_usd
from raw_spend_agg
left join channel_mapping
    on trim(raw_spend_agg.channel) = trim(channel_mapping.raw_channel)
{% if is_incremental() %}
  where unique_id not in (select unique_id from {{ this }})
{% endif %}
)

select * from final

