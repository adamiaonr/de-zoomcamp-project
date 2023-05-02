{{ config(
    materialized = "table"
) }}

select
    BusLineId,
    BusLineName,
    BusLineDirection,
    BusLineOrigin,
    BusLineDestination
from {{ ref('stg_bus_records') }}
group by BusLineId, BusLineName, BusLineDirection, BusLineOrigin, BusLineDestination
