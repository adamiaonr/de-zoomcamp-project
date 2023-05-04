{{ config(
    materialized = "table",
    partition_by = { "field": "DateMonth", "data_type": "date", "granularity": "month" },
    cluster_by = [ "BusLineId" ]
) }}

with line_delay as (
    select *
    from {{ metrics.calculate(
        metric('average_delay_per_bus_line_weather'),
        grain='month',
        dimensions=['BusLineId', 'Weather']
    ) }}
),

bus_lines as (
    select BusLineId, BusLineName, BusLineDirection
    from {{ ref('bus_delays') }}
    group by BusLineId, BusLineName, BusLineDirection
)

select
    -- month info
    date_month as DateMonth,
    {{ dbt_date.month_name('date_month') }} as MonthName,
    -- bus line info
    line_delay.BusLineId as BusLineId,
    BusLineName,
    -- weather info
    Weather,
    -- average delay
    average_delay_per_bus_line_weather as AverageDelay
from line_delay
inner join bus_lines
    on bus_lines.BusLineId = line_delay.BusLineId
