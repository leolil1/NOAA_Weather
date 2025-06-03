{% macro get_frshtt(frshtt) %}

    case frshtt
        when 0 then 'No severe weather'
        when 1 then 'Tornado'
        when 10 then 'Thunder'
        when 100 then 'Hail'
        when 1000 then 'Rain'
        when 10000 then 'Fog'
        else 'EMPTY'
    end
{% endmacro %}