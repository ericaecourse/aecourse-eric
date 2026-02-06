{{ config(schema="mart", tags=["mart"]) }}

WITH nested AS (
  SELECT *, SPLIT(TRIM(full_name), ' ') AS name_parts
  FROM {{ ref('stg_customers') }}
)

select
    customer_id,
    full_name,
    -- Extract first name (first element in the array)
    name_parts[SAFE_OFFSET(0)] AS first_name,
    -- Extract last name (join the remaining parts as last name)
    TRIM(ARRAY_TO_STRING(ARRAY_SLICE(name_parts, 1, ARRAY_LENGTH(name_parts)), ' ')) AS last_name,
    email,
    country_code,
    case
        when lower(country_code) IN ('us','usa') then true
        else false
    end as is_usa,
    created_ts_utc,
    date(created_ts_utc) as registered_date
from nested