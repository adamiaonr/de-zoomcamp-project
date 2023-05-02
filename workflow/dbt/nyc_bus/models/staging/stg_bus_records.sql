{{ config(materialized='view') }}

select
    -- id
    {{ dbt_utils.generate_surrogate_key(['RecordedAtTime', 'PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName', 'VehicleRef']) }} as RecordId,
    -- timestamps
    cast(RecordedAtTime as timestamp) as RecordDateTime,
    cast(ExpectedArrivalTime as timestamp) as ExpectedArrivalDateTime,
    cast(ScheduledArrivalTime as timestamp) as ScheduledArrivalDateTime,
    -- extra date & time info
    date_trunc(cast(RecordedAtTime as timestamp), hour) as RecordDateHour,
    cast(cast(RecordedAtTime as timestamp) as date) as RecordDate,
    -- bus line id
    {{ dbt_utils.generate_surrogate_key(['PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName']) }} as BusLineId,
    -- bus line info
    PublishedLineName as BusLineName,
    DirectionRef as BusLineDirection,
    OriginName as BusLineOrigin,
    DestinationName as BusLineDestination,
    -- vehicle info
    VehicleRef as VehicleId,
    -- geographical & location info
    st_geogpoint(OriginLong, OriginLat) as OriginLocation,
    st_geogpoint(DestinationLong, DestinationLat) as DestinationLocation,
    st_geogpoint(VehicleLocationLongitude, VehicleLocationLatitude) as VehicleLocation,
    NextStopPointName,
    -- bus stop id
    {{ dbt_utils.generate_surrogate_key(['PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName', 'NextStopPointName']) }} as BusStopId,
    -- status info
    ArrivalProximityText
from {{ source('staging', 'bus_records') }}
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
