with
source as(
    select * from {{ ref('stg__temp_trend_by_station') }}
)

SELECT  
    PARSE_DATE('%Y', CAST(date_year AS STRING)) AS date_year,
    avg_temp_f
FROM (
    SELECT
        date_year,
        ROUND(AVG(avg_daily_temp_f),2) as avg_temp_f
    FROM source
    WHERE date_year>1980
    GROUP BY date_year
    ORDER BY date_year
)
ORDER BY date_year