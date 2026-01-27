{{ config(schema="staging", materialized="incremental", unique_id="unique_id", tags=["staging"]) }}

with raw_customers as (
    select * from {{ ref('raw_customers_batch1') }}
    union all
    select * from {{ ref('raw_customers_batch2') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id'
    ]) }} as unique_id,
    customer_id,
    INITCAP(trim(full_name)) as full_name,
    LOWER(trim(email)) as email,
    UPPER(country_code) as country_code,
    {{ try_to_ts('created_ts') }} as created_ts_utc,
    {{ try_to_ts('updated_at') }} as updated_at_utc
from raw_customers
{% if is_incremental() %}
  where unique_id not in (select unique_id from {{ this }})
{% endif %}