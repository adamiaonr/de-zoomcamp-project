{{ config(
    materialized = "table",
    partition_by = { "field": "DateMonth", "data_type": "date", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

with bus_lines as (
    select *
    from {{ ref('int_bus_lines') }}
),

bus_stops as (
    select *
    from {{ ref('int_bus_stops') }}
),

line_delay as (
    select *
    from {{ metrics.calculate(
        metric('average_delay_per_bus_line_stop_per_weekday'),
        grain='month',
        dimensions=['BusLineId', 'BusStopId', 'DayOfWeek', 'DayOfWeekName']
    ) }}
)

select
    date_month as DateMonth,
    format_date('%B', date_month) as Month,
    line_delay.BusLineId as BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination,
    line_delay.BusStopId as BusStopId,
    BusStopName,
    BusStopLocation,
    DayOfWeek,
    DayOfWeekName,
    average_delay_per_bus_line_stop_per_weekday as AverageDelay,
from line_delay
inner join bus_lines
    on bus_lines.BusLineId = line_delay.BusLineId
inner join bus_stops
    on bus_stops.BusStopId = line_delay.BusStopId
