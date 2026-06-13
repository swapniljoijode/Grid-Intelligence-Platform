{{
  config(
    materialized  = 'table',
    schema        = 'gold',
    tags          = ['gold', 'fact'],
    incremental_strategy = 'merge',
    unique_key    = 'demand_key'
  )
}}

select
    s.demand_key,
    d.date_key,
    r.region_key,
    s.period_utc,
    s.period_hour,
    s.respondent,
    s.demand_mwh,
    s.demand_mw,
    sysdatetime()                           as _loaded_at
from {{ ref('stg_demand') }}    s
left join {{ ref('dim_date') }}  d on cast(s.period_utc as date) = d.date_day
left join {{ ref('dim_region') }} r on s.respondent = r.respondent_code

{% if is_incremental() %}
where s._cleansed_at > (
    select coalesce(max(_loaded_at), '1900-01-01') from {{ this }}
)
{% endif %}
