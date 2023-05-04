{{ config(
    materialized = "table",
    partition_by = { "field": "DateMonth", "data_type": "date", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

with line_delay as (
    select *
    from {{ metrics.calculate(
        metric('average_delay_per_bus_line_stop_per_weekday'),
        grain='month',
        dimensions=['BusLineId', 'BusStopId', 'DayOfWeek']
    ) }}
),

bus_stops as (
    select BusStopId, BusStopLocation
    from {{ ref('int_bus_stops') }}
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
    -- day of week info
    DayOfWeek,
    {{ get_day_of_week_from_number('DayOfWeek') }} as DayOfWeekName,
    -- bus line info
    line_delay.BusLineId as BusLineId,
    BusLineName,
    BusLineDirection,
    -- bus stop info
    line_delay.BusStopId as BusStopId,
    BusStopLocation,
    -- average delay
    average_delay_per_bus_line_stop_per_weekday as AverageDelay
from line_delay
inner join bus_stops
    on bus_stops.BusStopId = line_delay.BusStopId
inner join bus_lines
    on bus_lines.BusLineId = line_delay.BusLineId
