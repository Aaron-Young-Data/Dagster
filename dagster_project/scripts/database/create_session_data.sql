DROP TABLE IF EXISTS session_data;

CREATE TABLE session_data (
DRIVER varchar(3),
DRIVER_NUMBER int,
TEAM varchar(20),
EVENT_CD int,
SESSION_CD varchar(20),
LAPTIME float,
SECTOR1_TIME float,
SECTOR2_TIME float,
SECTOR3_TIME float,
COMPOUND varchar(20),
AIR_TEMP float,
RAINFALL_FLAG int,
TRACK_TEMP float,
WIND_DIRECTION int,
WIND_SPEED float,
LOAD_TS datetime
)