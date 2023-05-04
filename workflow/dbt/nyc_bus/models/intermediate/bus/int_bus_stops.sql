{{ config(
    materialized = "table"
) }}

select
    BusStopId,
    BusLineId,
    BusStopName,
    st_centroid_agg(VehicleLocation) as BusStopLocation
from {{ ref('stg_bus_records') }}
where BusStatus = 'at stop'
group by BusStopId, BusLineId, BusStopName
