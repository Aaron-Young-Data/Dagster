DROP VIEW IF EXISTS CLEANED_SESSION_DATA;

CREATE VIEW CLEANED_SESSION_DATA AS (
WITH session_data_temp AS (
SELECT
    DRIVER AS DRIVER,
    DRIVER_NUMBER AS DRIVER_NUMBER,
    TEAM AS TEAM,
    EVENT_CD AS EVENT_CD,
    CASE WHEN SESSION_CD = 'Practice 1' THEN LAPTIME END AS LAPTIME_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN SECTOR1_TIME END AS SECTOR1_TIME_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN SECTOR2_TIME END AS SECTOR2_TIME_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN SECTOR3_TIME END AS SECTOR3_TIME_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN COMPOUND END AS COMPOUND_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN AIR_TEMP END AS AIR_TEMP_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN RAINFALL_FLAG END AS RAINFALL_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN TRACK_TEMP END AS TRACK_TEMP_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN WIND_DIRECTION END AS WIND_DIRECTION_FP1,
    CASE WHEN SESSION_CD = 'Practice 1' THEN WIND_SPEED END AS WIND_SPEED_FP1,
    CASE WHEN SESSION_CD = 'Practice 2' THEN LAPTIME END AS LAPTIME_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN SECTOR1_TIME END AS SECTOR1_TIME_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN SECTOR2_TIME END AS SECTOR2_TIME_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN SECTOR3_TIME END AS SECTOR3_TIME_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN COMPOUND END AS COMPOUND_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN AIR_TEMP END AS AIR_TEMP_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN RAINFALL_FLAG END AS RAINFALL_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN TRACK_TEMP END AS TRACK_TEMP_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN WIND_DIRECTION END AS WIND_DIRECTION_FP2,
    CASE WHEN SESSION_CD = 'Practice 2' THEN WIND_SPEED END AS WIND_SPEED_FP2,
    CASE WHEN SESSION_CD = 'Practice 3' THEN LAPTIME END AS LAPTIME_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN SECTOR1_TIME END AS SECTOR1_TIME_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN SECTOR2_TIME END AS SECTOR2_TIME_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN SECTOR3_TIME END AS SECTOR3_TIME_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN COMPOUND END AS COMPOUND_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN AIR_TEMP END AS AIR_TEMP_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN RAINFALL_FLAG END AS RAINFALL_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN TRACK_TEMP END AS TRACK_TEMP_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN WIND_DIRECTION END AS WIND_DIRECTION_FP3,
    CASE WHEN SESSION_CD = 'Practice 3' THEN WIND_SPEED END AS WIND_SPEED_FP3,
    CASE WHEN SESSION_CD = 'Qualifying' THEN LAPTIME END AS LAPTIME_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN SECTOR1_TIME END AS SECTOR1_TIME_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN SECTOR2_TIME END AS SECTOR2_TIME_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN SECTOR3_TIME END AS SECTOR3_TIME_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN COMPOUND END AS COMPOUND_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN AIR_TEMP END AS AIR_TEMP_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN RAINFALL_FLAG END AS RAINFALL_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN TRACK_TEMP END AS TRACK_TEMP_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN WIND_DIRECTION END AS WIND_DIRECTION_Q,
    CASE WHEN SESSION_CD = 'Qualifying' THEN WIND_SPEED END AS WIND_SPEED_Q 
FROM session_data)

SELECT 
    dat.EVENT_CD AS EVENT_CD,
    dat.DRIVER AS DRIVER,
    dat.DRIVER_NUMBER AS DRIVER_NUMBER,
    dat.TEAM AS TEAM,
    round(sum(dat.LAPTIME_FP1),2) AS LAPTIME_FP1,
    round(sum(dat.SECTOR1_TIME_FP1),2) AS SECTOR1_TIME_FP1,
    round(sum(dat.SECTOR2_TIME_FP1),2) AS SECTOR2_TIME_FP1,
    round(sum(dat.SECTOR3_TIME_FP1),2) AS SECTOR3_TIME_FP1,
    round(sum(dat.COMPOUND_FP1),2) AS COMPOUND_FP1,
    round(sum(dat.AIR_TEMP_FP1),2) AS AIR_TEMP_FP1,
    round(sum(dat.RAINFALL_FP1),2) AS RAINFALL_FP1,
    round(sum(dat.TRACK_TEMP_FP1),2) AS TRACK_TEMP_FP1,
    round(sum(dat.WIND_DIRECTION_FP1),2) AS WIND_DIRECTION_FP1,
    round(sum(dat.WIND_SPEED_FP1),2) AS WIND_SPEED_FP1,
    round(sum(dat.LAPTIME_FP2),2) AS LAPTIME_FP2,
    round(sum(dat.SECTOR1_TIME_FP2),2) AS SECTOR1_TIME_FP2,
    round(sum(dat.SECTOR2_TIME_FP2),2) AS SECTOR2_TIME_FP2,
    round(sum(dat.SECTOR3_TIME_FP2),2) AS SECTOR3_TIME_FP2,
    round(sum(dat.COMPOUND_FP2),2) AS COMPOUND_FP2,
    round(sum(dat.AIR_TEMP_FP2),2) AS AIR_TEMP_FP2,
    round(sum(dat.RAINFALL_FP2),2) AS RAINFALL_FP2,
    round(sum(dat.TRACK_TEMP_FP2),2) AS TRACK_TEMP_FP2,
    round(sum(dat.WIND_DIRECTION_FP2),2) AS WIND_DIRECTION_FP2,
    round(sum(dat.WIND_SPEED_FP2),2) AS WIND_SPEED_FP2,
    round(sum(dat.LAPTIME_FP3),2) AS LAPTIME_FP3,
    round(sum(dat.SECTOR1_TIME_FP3),2) AS SECTOR1_TIME_FP3,
    round(sum(dat.SECTOR2_TIME_FP3),2) AS SECTOR2_TIME_FP3,
    round(sum(dat.SECTOR3_TIME_FP3),2) AS SECTOR3_TIME_FP3,
    round(sum(dat.COMPOUND_FP3),2) AS COMPOUND_FP3,
    round(sum(dat.AIR_TEMP_FP3),2) AS AIR_TEMP_FP3,
    round(sum(dat.RAINFALL_FP3),2) AS RAINFALL_FP3,
    round(sum(dat.TRACK_TEMP_FP3),2) AS TRACK_TEMP_FP3,
    round(sum(dat.WIND_DIRECTION_FP3),2) AS WIND_DIRECTION_FP3,
    round(sum(dat.WIND_SPEED_FP3),2) AS WIND_SPEED_FP3,
    round(sum(dat.LAPTIME_Q),2) AS LAPTIME_Q,
    round(sum(dat.SECTOR1_TIME_Q),2) AS SECTOR1_TIME_Q,
    round(sum(dat.SECTOR2_TIME_Q),2) AS SECTOR2_TIME_Q,
    round(sum(dat.SECTOR3_TIME_Q),2) AS ASSECTOR3_TIME_Q,
    round(sum(dat.COMPOUND_Q),2) AS COMPOUND_Q,
    round(sum(dat.AIR_TEMP_Q),2) AS AIR_TEMP_Q,
    round(sum(dat.RAINFALL_Q),2) AS RAINFALL_Q,
    round(sum(dat.TRACK_TEMP_Q),2) AS TRACK_TEMP_Q,
    round(sum(dat.WIND_DIRECTION_Q),2) AS WIND_DIRECTION_Q,
    round(sum(dat.WIND_SPEED_Q),2) AS WIND_SPEED_Q 
FROM session_data_temp dat 
LEFT JOIN dim_event evt 
    ON evt.EVENT_CD = dat.EVENT_CD
GROUP BY
    dat.EVENT_CD,
    dat.DRIVER,
    dat.DRIVER_NUMBER,
    dat.TEAM,
    evt.EVENT_YEAR,
    evt.ROUND_NUMBER 
ORDER BY
    evt.EVENT_YEAR,
    evt.ROUND_NUMBER,
    round(sum(dat.LAPTIME_Q),2)
)