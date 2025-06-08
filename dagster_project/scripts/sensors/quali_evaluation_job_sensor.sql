SELECT
count(*) AS RowCount
FROM SESSION.QUALIFYING_RESULTS dat
JOIN REFERENCE.DIM_EVENT evt
	ON dat.EVENT_CD = evt.EVENT_CD
WHERE
    evt.EVENT_YEAR = '{event_year}'
    AND evt.ROUND_NUMBER = '{round_number}'
    AND SESSION_CD = 4