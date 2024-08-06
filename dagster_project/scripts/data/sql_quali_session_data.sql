SELECT
	CONCAT(DAT.DRIVER, '-', DAT.DRIVER_NUMBER) AS DRIVER,
	DAT.LAPTIME_Q AS LAPTIME,
	RANK() OVER (ORDER BY DAT.LAPTIME_Q) AS FINAL_POS
FROM CLEANED_SESSION_DATA DAT
LEFT JOIN DIM_EVENT EVT
	ON EVT.EVENT_CD = DAT.EVENT_CD
WHERE
	DAT.EVENT_CD = '{event_cd}'
    AND DAT.LAPTIME_Q IS NOT NULL