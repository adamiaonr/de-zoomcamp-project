{{ config(
    materialized = "incremental",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

select
    -- id
    RecordId,
    -- timestamps
    RecordDateTime as RecordDateTime,
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
    timestamp_diff(ExpectedArrivalDateTime, ScheduledArrivalDateTime, second) as DelaySeconds
from {{ ref('stg_bus_records') }}
where
    ExpectedArrivalDateTime is not null
    and ScheduledArrivalDateTime is not null
    -- we're only interested in the delay 'at the stop'
    and ArrivalProximityText = 'at stop'
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and RecordDateTime >= (select max(RecordDateTime) from {{ this }})
{% endif %}
