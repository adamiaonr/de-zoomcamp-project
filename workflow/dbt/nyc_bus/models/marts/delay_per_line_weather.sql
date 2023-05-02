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
    select *
    from {{ ref('int_bus_lines') }}
)

select
    date_month as DateMonth,
    format_date('%B', date_month) as Month,
    line_delay.BusLineId as BusLineId,
    BusLineName,
    Weather,
    average_delay_per_bus_line_weather as AverageDelay,
from line_delay
inner join bus_lines
    on bus_lines.BusLineId = line_delay.BusLineId
