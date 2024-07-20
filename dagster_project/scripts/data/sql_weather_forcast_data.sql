select
datetime_utc,
temperature AS AirTempQ,
CASE WHEN precipitation_prob >= 60 THEN 1 ELSE 0 END as RainfallQ,
wind_direction AS WindDirectionQ,
wind_speed AS WindSpeedQ
from ml_project_prod.weather_forcast
where event_name = '{event}' and
datetime_utc >= '{session_date_time}'