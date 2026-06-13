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
            convert(varchar, period_utc, 126), '|', respondent, '|', fuel_type
        ))))
    )                                   as generation_key,
    period_utc,
    cast(period_utc as date)            as period_date,
    datepart(hour, period_utc)          as period_hour,
    respondent,
    fuel_type,
    fuel_type_label,
    generation_mwh,
    _cleansed_at
from {{ source('silver', 'generation') }}
