SELECT
count(*) AS RowCount
FROM session_data dat
JOIN dim_event evt
	ON dat.EVENT_CD = evt.EVENT_CD
WHERE
	evt.EVENT_NAME = '{event_name}'
    AND evt.EVENT_YEAR = '{event_year}'
    AND SESSION_CD = 'Qualifying';