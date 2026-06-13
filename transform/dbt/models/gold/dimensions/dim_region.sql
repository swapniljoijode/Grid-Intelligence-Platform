{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension']
  )
}}

/*
  Static region dimension derived from data observed in Silver.
  Supplements known static regions with any new respondents found in demand.
*/
select
    convert(int, row_number() over (order by respondent_code)) as region_key,
    respondent_code,
    region_name,
    country,
    timezone_name,
    grid_type
from (
    values
        ('ERCO', 'Electric Reliability Council of Texas', 'United States', 'America/Chicago', 'RTO'),
        ('GB',   'Great Britain National Grid',           'United Kingdom', 'Europe/London',  'TSO'),
        ('CAISO','California Independent System Operator','United States', 'America/Los_Angeles', 'ISO'),
        ('MISO', 'Midcontinent Independent System Operator','United States','America/Chicago','RTO'),
        ('PJM',  'PJM Interconnection',                  'United States', 'America/New_York','RTO'),
        ('NYISO','New York Independent System Operator', 'United States', 'America/New_York','ISO'),
        ('ISONE','ISO New England',                      'United States', 'America/New_York','ISO'),
        ('SPP',  'Southwest Power Pool',                 'United States', 'America/Chicago', 'RTO'),
        ('SWPP', 'Southwest Power Pool (alt)',           'United States', 'America/Chicago', 'RTO')
) as r (respondent_code, region_name, country, timezone_name, grid_type)
