{{ config(
    materialized = "table"
) }}

select
    BusStopId,
    BusLineId,
    NextStopPointName as BusStopName,
    st_centroid_agg(VehicleLocation) as BusStopLocation
from {{ ref('stg_bus_records') }}
where ArrivalProximityText = 'at stop'
group by BusStopId, BusLineId, NextStopPointName
