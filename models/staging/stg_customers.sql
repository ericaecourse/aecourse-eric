{{ config(schema="staging", tags=["staging"]) }}

with raw_customers as (
    select * from {{ ref('raw_customers_batch1')}}
)

select
    customer_id,
    INITCAP(trim(full_name)) as full_name,
    LOWER(trim(email)) as email,
    UPPER(country_code) as country_code,
    {{ try_to_ts('created_ts') }} as created_ts_utc,
    {{ try_to_ts('updated_at') }} as updated_at_utc
from raw_customers