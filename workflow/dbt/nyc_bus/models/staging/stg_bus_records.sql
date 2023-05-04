{{ config(materialized='view') }}

select
    -- id
    {{ dbt_utils.generate_surrogate_key(['RecordedAtTime', 'PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName', 'VehicleRef']) }} as RecordId,
    -- timestamps
    cast(RecordedAtTime as timestamp) as RecordDateTime,
    cast(ExpectedArrivalTime as timestamp) as ExpectedArrivalDateTime,
    cast(ScheduledArrivalTime as timestamp) as ScheduledArrivalDateTime,
    -- bus line id & info
    {{ dbt_utils.generate_surrogate_key(['PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName']) }} as BusLineId,
    PublishedLineName as BusLineName,
    DirectionRef as BusLineDirection,
    OriginName as BusLineOrigin,
    DestinationName as BusLineDestination,
    -- geographical location of vehicle
    st_geogpoint(VehicleLocationLongitude, VehicleLocationLatitude) as VehicleLocation,
    -- bus stop info
    {{ dbt_utils.generate_surrogate_key(['PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName', 'NextStopPointName']) }} as BusStopId,
    NextStopPointName as BusStopName,
    -- bus status (e.g., 'at stop', 'approaching')
    ArrivalProximityText as BusStatus
from {{ source('staging', 'bus_records') }}
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
