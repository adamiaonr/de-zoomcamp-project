{{ config(materialized='view') }}

-- unpivot table
with unpivoted as
(
    {{ dbt_utils.unpivot(
        relation=source('staging', 'weather_description'),
        cast_to='string',
        exclude=['datetime'],
        field_name='City',
        value_name='Weather'
    ) }}
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['datetime', 'City']) }} as RecordId,
    -- timestamps
    cast(datetime as timestamp) as RecordDateTime,
    -- city
    City,
    -- value
    Weather
from unpivoted

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
