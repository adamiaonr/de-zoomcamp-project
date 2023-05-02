{{ config(
    materialized = "table",
    partition_by = { "field": "RecordDateTime", "data_type": "timestamp", "granularity": "day" },
) }}

with humidity as (
    select
        RecordDateTime,
        Humidity
    from {{ ref('stg_humidity') }}
    where City = 'NewYork'
),

temperature as (
    select
        RecordDateTime,
        Temperature
    from {{ ref('stg_temperature') }}
    where City = 'NewYork'
),

weather_description as (
    select
        RecordDateTime,
        Weather
    from {{ ref('stg_weather_description') }}
    where City = 'NewYork'
)

select
    -- time
    temperature.RecordDateTime as RecordDateTime,
    -- weather variables
    humidity.Humidity as Humidity,
    temperature.Temperature as Temperature,
    Weather
from temperature
inner join humidity
on temperature.RecordDateTime = humidity.RecordDateTime
inner join weather_description
on humidity.RecordDateTime = weather_description.RecordDateTime
