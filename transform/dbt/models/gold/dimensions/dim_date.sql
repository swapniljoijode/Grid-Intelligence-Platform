{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension']
  )
}}

/*
  Date spine using cross-join row generator — avoids recursive CTE which
  requires OPTION(MAXRECURSION) incompatible with CREATE TABLE AS SELECT.
  Covers ercot_start_date through today + 365 days (forecast buffer).
*/
select
    convert(int, format(date_day, 'yyyyMMdd'))          as date_key,
    date_day,
    year(date_day)                                       as year,
    month(date_day)                                      as month,
    day(date_day)                                        as day_of_month,
    datepart(quarter, date_day)                          as quarter,
    datepart(week, date_day)                             as week_of_year,
    datepart(weekday, date_day)                          as day_of_week,
    datename(weekday, date_day)                          as day_name,
    datename(month, date_day)                            as month_name,
    format(date_day, 'yyyy-MM')                          as year_month,
    cast(
        case when datepart(weekday, date_day) in (1, 7)
        then 1 else 0 end
    as bit)                                              as is_weekend,
    cast(
        case when date_day = cast(getdate() as date)
        then 1 else 0 end
    as bit)                                              as is_today
from (
    select top (
        datediff(
            day,
            cast('{{ var("ercot_start_date") }}' as date),
            getdate()
        ) + 366
    )
    dateadd(
        day,
        row_number() over (order by (select null)) - 1,
        cast('{{ var("ercot_start_date") }}' as date)
    ) as date_day
    from sys.all_objects a
    cross join sys.all_objects b
) as spine
