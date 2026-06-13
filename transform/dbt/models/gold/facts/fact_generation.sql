{{
  config(
    materialized  = 'table',
    schema        = 'gold',
    tags          = ['gold', 'fact'],
    incremental_strategy = 'merge',
    unique_key    = 'generation_key'
  )
}}

select
    s.generation_key,
    d.date_key,
    r.region_key,
    f.fuel_type_key,
    s.period_utc,
    s.period_hour,
    s.respondent,
    s.fuel_type,
    s.fuel_type_label,
    s.generation_mwh,
    -- estimated CO2 emissions using fuel type benchmark
    round(s.generation_mwh * coalesce(f.typical_co2_gco2_kwh, 0) / 1000.0, 2)
                                            as estimated_co2_tonnes,
    cast(
        case when coalesce(f.is_renewable, 0) = 1 then 1 else 0 end
    as bit)                                 as is_renewable,
    sysdatetime()                           as _loaded_at
from {{ ref('stg_generation') }}  s
left join {{ ref('dim_date') }}    d on cast(s.period_utc as date) = d.date_day
left join {{ ref('dim_region') }}  r on s.respondent = r.respondent_code
left join {{ ref('dim_fuel_type') }} f on s.fuel_type = f.fuel_type_code

{% if is_incremental() %}
where s._cleansed_at > (
    select coalesce(max(_loaded_at), '1900-01-01') from {{ this }}
)
{% endif %}
