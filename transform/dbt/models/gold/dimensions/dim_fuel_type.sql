{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension']
  )
}}

/*
  EIA fuel type dimension. Includes carbon intensity benchmarks (gCO2/kWh)
  for use in emissions calculations and portfolio analysis.
*/
select
    convert(int, row_number() over (order by fuel_type_code)) as fuel_type_key,
    fuel_type_code,
    fuel_type_label,
    fuel_category,
    cast(is_renewable as bit)       as is_renewable,
    cast(is_low_carbon as bit)      as is_low_carbon,
    typical_co2_gco2_kwh
from (
    values
        ('COL', 'Coal',              'Thermal',     0, 0, 820.0),
        ('NG',  'Natural Gas',       'Thermal',     0, 0, 490.0),
        ('OIL', 'Oil',               'Thermal',     0, 0, 650.0),
        ('NUC', 'Nuclear',           'Low-Carbon',  0, 1,  12.0),
        ('WAT', 'Hydro',             'Renewable',   1, 1,   4.0),
        ('WND', 'Wind',              'Renewable',   1, 1,  11.0),
        ('SUN', 'Solar',             'Renewable',   1, 1,  45.0),
        ('GEO', 'Geothermal',        'Renewable',   1, 1,  38.0),
        ('BIO', 'Biomass',           'Renewable',   1, 0, 230.0),
        ('OTH', 'Other',             'Other',       0, 0,   0.0),
        ('UNK', 'Unknown',           'Unknown',     0, 0,   0.0)
) as f (fuel_type_code, fuel_type_label, fuel_category, is_renewable, is_low_carbon, typical_co2_gco2_kwh)
