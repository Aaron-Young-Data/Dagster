SELECT
    EVT.EVENT_NAME,
    EVT.FCST_LOCATION,
    TRK.LATITUDE,
    TRK.LONGITUDE
FROM
    DIM_EVENT EVT
LEFT JOIN DIM_TRACK TRK
    ON EVT.TRACK_CD = TRK.TRACK_CD
WHERE
    EVENT_YEAR = '{partitioned_date_year}'
GROUP BY
    EVT.EVENT_NAME,
    EVT.FCST_LOCATION,
    TRK.LATITUDE,
    TRK.LONGITUDE
