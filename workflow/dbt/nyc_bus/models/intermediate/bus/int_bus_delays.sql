{{ config(
    materialized = "incremental",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "month" },
    cluster_by = [ "BusLineId", "BusStopId", "DayOfWeek" ]
) }}

select
    -- id
    RecordId,
    -- timestamp
    RecordDateTime,
    -- timestamp variants
    extract(dayofweek from cast(RecordDateTime as date)) as DayOfWeek,
    -- bus line id
    BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination
    -- bus stop Id
    BusStopId,
    -- bus delay
    timestamp_diff(ExpectedArrivalDateTime, ScheduledArrivalDateTime, second) as DelaySeconds,
from {{ ref('stg_bus_records') }} bus_records
where
    ExpectedArrivalDateTime is not null
    and ScheduledArrivalDateTime is not null
    -- we're only interested in the delay 'at the stop'
    and BusStatus = 'at stop'
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and RecordDateTime >= (select max(RecordDateTime) from {{ this }})
{% endif %}
