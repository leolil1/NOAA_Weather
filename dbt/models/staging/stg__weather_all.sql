with 

source as (

    select * from {{ source('staging', 'weather_all') }}

)

select
    station,
    date,
    extract (year from date) as date_year,
    elevation,
    temp,
    dewp,
    visib,
    wdsp,
    mxspd,
    gust,
    NULLIF(max, 9999.9)  as max_temp,
    NULLIF(min, 9999.9) as min_temp,
    prcp,
    sndp,
    {{get_frshtt('frshtt')}} as severe_weather
from source 


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}