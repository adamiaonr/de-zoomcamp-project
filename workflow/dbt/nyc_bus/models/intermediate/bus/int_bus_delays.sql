{{ config(
    materialized = "incremental",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

with weather_nyc as (
    select *
    from {{ ref('int_weather_nyc') }}
)

select
    -- id
    RecordId,
    -- timestamps
    bus_records.RecordDateTime as RecordDateTime,
    ExpectedArrivalDateTime,
    ScheduledArrivalDateTime,
    -- other date & time info
    RecordDate,
    RecordDateHour,
    extract(dayofweek from RecordDate) as DayOfWeek,
    format_date('%A', RecordDate) as DayOfWeekName,
    -- bus line id
    BusLineId,
    -- geographical & location info
    BusStopId,
    -- bus delay
    timestamp_diff(ExpectedArrivalDateTime, ScheduledArrivalDateTime, second) as DelaySeconds,
    -- weather info
    Weather,
    Humidity,
    Temperature
from {{ ref('stg_bus_records') }} bus_records
inner join weather_nyc
    on weather_nyc.RecordDateTime = bus_records.RecordDateHour
where
    ExpectedArrivalDateTime is not null
    and ScheduledArrivalDateTime is not null
    -- we're only interested in the delay 'at the stop'
    and ArrivalProximityText = 'at stop'
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and bus_records.RecordDateTime >= (select max(RecordDateTime) from {{ this }})
{% endif %}
