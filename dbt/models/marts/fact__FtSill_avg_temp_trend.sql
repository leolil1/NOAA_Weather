with
source as(
    select * from {{ ref('stg__temp_trend_by_station') }}
)

SELECT
    PARSE_DATE('%Y', CAST(date_year AS STRING)) AS date_year,
    avg_daily_temp_f as avg_temp_f
FROM source
WHERE station='72355013945'
ORDER BY date_year