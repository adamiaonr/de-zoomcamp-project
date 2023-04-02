{{ config(materialized='view') }}

select
    -- timestamps (NY timezone)
    cast(RecordedAtTime as timestamp) as RecordedAtTime,
    cast(ExpectedArrivalTime as timestamp) as ExpectedArrivalTime,
    ScheduledArrivalTime,
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
from {{source('staging', 'raw_records')}}
order by RecordedAtTime
limit 1000
