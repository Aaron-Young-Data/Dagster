select
FCST_DATETIME AS datetime_utc,
temperature AS AirTempQ,
CASE WHEN PRECIPITATION_PROB >= 60 THEN 1 ELSE 0 END as RainfallQ,
wind_direction AS WindDirectionQ,
wind_speed AS WindSpeedQ
from weather_forecast fcst
left join dim_event evt
	on fcst.FCST_LOCATION = evt.FCST_LOCATION
where evt.event_name = '{event}' and
FCST_DATETIME >= '{session_date_time}'