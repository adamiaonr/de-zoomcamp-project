{#
    returns the abbreviated day of the week string, given its number.
    assumes Sunday is 1.
#}

{% macro get_day_of_week_from_number(day_of_week_number) -%}

    case {{ day_of_week_number }}
        when 1 then 'Sun'
        when 2 then 'Mon'
        when 3 then 'Tue'
        when 4 then 'Wed'
        when 5 then 'Thu'
        when 6 then 'Fri'
        when 7 then 'Sat'
    end

{%- endmacro %}
