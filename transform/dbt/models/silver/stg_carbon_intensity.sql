{{
  config(
    materialized = 'view',
    schema       = 'silver',
    tags         = ['silver']
  )
}}

select
    convert(
        bigint,
        abs(convert(bigint, hashbytes('MD5', concat(
            convert(varchar, period_from, 126), '|', convert(varchar, period_to, 126)
        ))))
    )                                       as carbon_intensity_key,
    period_from,
    period_to,
    cast(period_from as date)               as period_date,
    intensity_gco2_kwh,
    intensity_index,
    cast(is_estimated as bit)               as is_estimated,
    cast(duration_minutes as int)           as duration_minutes,
    _cleansed_at
from {{ source('silver', 'carbon_intensity') }}
