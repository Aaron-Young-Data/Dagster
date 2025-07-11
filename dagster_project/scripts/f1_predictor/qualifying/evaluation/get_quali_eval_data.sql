SELECT
    CONCAT(DRI.DRIVER_CODE, '-', DRI.DRIVER_NUMBER) AS DRIVER,
    CONCAT('#',CON.CONSTRUCTOR_COLOUR) AS CONSTRUCTOR_COLOUR,
    CAST(@s:=@s+1 AS UNSIGNED) AS PREDICTED_POSITION,
    ACT.Q_POSITION AS ACTUAL_POSITION,
    PRED.PREDICTED_LAPTIME_Q,
    ROUND(ACT.Q_TIME - PRED.PREDICTED_LAPTIME_Q, 3) AS LAPTIME_DIFFRENCE,
    ABS(ROUND(ACT.Q_TIME - PRED.PREDICTED_LAPTIME_Q, 3)) AS ABS_LAPTIME_DIFFRENCE
FROM PREDICTION.QUALIFYING_PREDICTION_DATA PRED
LEFT JOIN (SELECT * FROM SESSION.QUALIFYING_RESULTS WHERE SESSION_CD = 4) ACT
    ON PRED.EVENT_CD = ACT.EVENT_CD
    AND PRED.DRIVER_ID = ACT.DRIVER_ID
LEFT JOIN REFERENCE.DIM_CONSTRUCTOR CON
    ON ACT.TEAM_ID = CON.CONSTRUCTOR_ID
LEFT JOIN REFERENCE.DIM_DRIVER DRI
    ON PRED.DRIVER_ID = DRI.DRIVER_ID, (select @s:=0) as s
WHERE PRED.EVENT_CD = {year}{round_num}