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
            convert(varchar, period_utc, 126), '|', respondent
        ))))
    )                                   as demand_key,
    period_utc,
    cast(period_utc as date)            as period_date,
    datepart(hour, period_utc)          as period_hour,
    respondent,
    respondent_name,
    demand_mwh,
    demand_mw,
    _cleansed_at
from {{ source('silver', 'demand') }}
