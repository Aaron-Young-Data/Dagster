SELECT
a.*,
b.*
FROM
tableau_data.all_session_data a
LEFT JOIN tableau_data.dim_track_status b
ON a.TrackStatus = b.TrackStatus_cd
WHERE
LapTime is not NULL