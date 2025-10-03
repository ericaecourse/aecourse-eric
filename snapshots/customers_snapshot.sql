{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email']
    )
}}

select
    customer_id,
    full_name,
    email,
    country_code,
    updated_at_utc
from {{ ref('stg_customers') }}

{% endsnapshot %}