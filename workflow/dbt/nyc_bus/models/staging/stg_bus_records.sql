{{ config(materialized='view') }}

select
    -- id
    {{ dbt_utils.generate_surrogate_key(['RecordedAtTime', 'PublishedLineName', 'DirectionRef', 'OriginName', 'DestinationName', 'VehicleRef']) }} as RecordId,
    -- timestamps
    cast(RecordedAtTime as timestamp) as RecordedAtTime,
    cast(ExpectedArrivalTime as timestamp) as ExpectedArrivalTime,
    cast(ScheduledArrivalTime as timestamp) as ScheduledArrivalTime,
    -- bus line identifiers
    DirectionRef,
    PublishedLineName,
    OriginName,
    DestinationName,
    -- vehicle info
    VehicleRef,
    -- geographical info
    st_geogpoint(OriginLong, OriginLat) as OriginLocation,
    st_geogpoint(DestinationLong, DestinationLat) as DestinationLocation,
    st_geogpoint(VehicleLocationLongitude, VehicleLocationLatitude) as VehicleLocation,
    NextStopPointName,
    -- status info
    ArrivalProximityText
from {{ source('staging', 'bus_records') }}
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
