SELECT
a.Driver,
a.DriverNumber,
a.Team,
a.LapTime,
a.LapNumber,
a.Position,
a.Stint,
a.TyreLife,
a.FreshTyre,
a.Sector1Time,
a.Sector2Time,
a.Sector3Time,
a.SpeedI1,
a.SpeedI2,
a.SpeedFL,
a.SpeedST,
a.IsPersonalBest,
a.Compound,
a.Deleted,
a.DeletedReason,
a.AirTemp,
a.Rainfall,
a.TrackTemp,
a.WindDirection,
a.WindSpeed,
b.TrackStatus,
a.IsAccurate,
a.Session,
a.event_name,
a.year,
a.event_type
FROM
TABLEAU_DATA.ALL_SESSION_DATA a
LEFT JOIN TABLEAU_DATA.DIM_TRACK_STATUS b
ON a.TrackStatus = b.TrackStatus_cd
WHERE
LapTime is not NULL