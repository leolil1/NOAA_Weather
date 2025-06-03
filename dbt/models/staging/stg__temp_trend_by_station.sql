with 

source as(
    select * from {{ ref('stg__weather_all') }}
),

dim_stationID as(
    select * from {{ ref('dim_stationID') }}
)

SELECT
    ROUND(AVG(source.temp),2) AS avg_daily_temp_f,
    ROUND(MAX(source.max_temp),2) AS yearly_max_temp_f,
    ROUND(MIN(source.min_temp),2) AS yearly_mix_temp_f,
    COUNTIF(source.max_temp > 95) AS days_above_95f,
    COUNTIF(source.min_temp < 32) AS days_below_32f,
    source.station, dim_stationID.station_name, dim_stationID.country, source.date_year,
from source
JOIN dim_stationID
ON
cast(source.station as string)=dim_stationID.stationID
group by source.date_year, source.station, dim_stationID.station_name, dim_stationID.country
order by source.date_year, dim_stationID.station_name

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}