select
    stationID,
    station_name,
    ctry as country,
    state
from {{ ref('stationIDList') }}