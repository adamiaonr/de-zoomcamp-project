{{ config(materialized='view') }}

-- unpivot table
with unpivoted as
(
    {{ dbt_utils.unpivot(
        relation=source('staging', 'temperature'),
        cast_to='float64',
        exclude=['datetime'],
        field_name='City',
        value_name='Temperature'
    ) }}
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['datetime', 'City']) }} as RecordId,
    -- timestamps
    cast(datetime as timestamp) as RecordDateTime,
    -- city
    City,
    -- value (convert from kelvin to celsius)
    (Temperature - 273.15) as Temperature
from unpivoted

{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
