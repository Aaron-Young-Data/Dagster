DROP TABLE IF EXISTS weather_forcast;

CREATE TABLE weather_forcast (
event_name varchar(30),
datetime_utc datetime,
temperature float,
precipitation float,
precipitation_prob float,
wind_speed float,
wind_direction float,
cloud_cover float,
conditions varchar(30),
source varchar(5),
load_datetime datetime
)
