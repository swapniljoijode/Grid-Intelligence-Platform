{{
  config(
    materialized  = 'table',
    schema        = 'gold',
    tags          = ['gold', 'fact'],
    incremental_strategy = 'merge',
    unique_key    = 'carbon_intensity_key'
  )
}}

select
    s.carbon_intensity_key,
    d.date_key,
    s.period_from,
    s.period_to,
    s.period_date,
    datepart(hour, s.period_from)           as period_hour,
    s.intensity_gco2_kwh,
    s.intensity_index,
    s.is_estimated,
    s.duration_minutes,
    -- classify intensity level for dashboard filtering
    case
        when s.intensity_gco2_kwh < 100  then 'Very Low'
        when s.intensity_gco2_kwh < 200  then 'Low'
        when s.intensity_gco2_kwh < 300  then 'Moderate'
        when s.intensity_gco2_kwh < 400  then 'High'
        else                                   'Very High'
    end                                     as intensity_band,
    cast(
        case when s.intensity_gco2_kwh < 150 then 1 else 0 end
    as bit)                                 as is_low_carbon_window,
    sysdatetime()                           as _loaded_at
from {{ ref('stg_carbon_intensity') }} s
left join {{ ref('dim_date') }} d on s.period_date = d.date_day

{% if is_incremental() %}
where s._cleansed_at > (
    select coalesce(max(_loaded_at), '1900-01-01') from {{ this }}
)
{% endif %}
