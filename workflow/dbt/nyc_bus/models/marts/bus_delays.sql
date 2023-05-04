{{ config(
    materialized = "table",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

with weather_nyc as (
    select *
    from {{ ref('int_weather_nyc') }}
),

bus_stops as (
    select *
    from {{ ref('int_bus_stops') }}
)

select
    -- id
    RecordId,
    -- timestamp
    bus_delays.RecordDateTime as RecordDateTime,
    -- timestamp variants
    DayOfWeek,
    -- bus line info
    bus_delays.BusLineId as BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination,
    -- bus stop info
    bus_delays.BusStopId as BusStopId,
    BusStopName,
    BusStopLocation,
    -- bus delay
    DelaySeconds,
    -- weather info
    Weather,
    Humidity,
    Temperature
from {{ ref('int_bus_delays') }} bus_delays
inner join weather_nyc
    on weather_nyc.RecordDateTime = date_trunc(cast(bus_delays.RecordDateTime as timestamp), hour)
inner join bus_stops
    on bus_stops.BusStopId = bus_delays.BusStopId
