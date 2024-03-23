DROP VIEW IF EXISTS cleaned_session_data;

CREATE VIEW cleaned_session_data AS
SELECT
evt.event_cd AS event_cd,
dtr.year AS year,
dtr.LapTimeFP1 AS LapTimeFP1,
dtr.Sector1TimeFP1 AS Sector1TimeFP1,
dtr.Sector2TimeFP1 AS Sector2TimeFP1,
dtr.Sector3TimeFP1 AS Sector3TimeFP1,
fp1comp.compound_cd AS CompoundFP1,
dtr.AirTempFP1 AS AirTempFP1,
dtr.RainfallFP1  AS RainfallFP1,
dtr.TrackTempFP1 AS TrackTempFP1,
dtr.WindDirectionFP1 AS WindDirectionFP1,
dtr.WindSpeedFP1 AS WindSpeedFP1,
dtr.LapTimeFP2 AS LapTimeFP2,
dtr.Sector1TimeFP2 AS Sector1TimeFP2,
dtr.Sector2TimeFP2 AS Sector2TimeFP2,
dtr.Sector3TimeFP2 AS Sector3TimeFP2,
fp2comp.compound_cd AS CompoundFP2,
dtr.AirTempFP2 AS AirTempFP2,
dtr.RainfallFP2  AS RainfallFP2,
dtr.TrackTempFP2 AS TrackTempFP2,
dtr.WindDirectionFP2 AS WindDirectionFP2,
dtr.WindSpeedFP2 AS WindSpeedFP2,
dtr.LapTimeFP3 AS LapTimeFP3,
dtr.Sector1TimeFP3 AS Sector1TimeFP3,
dtr.Sector2TimeFP3 AS Sector2TimeFP3,
dtr.Sector3TimeFP3 AS Sector3TimeFP3,
fp3comp.compound_cd AS CompoundFP3,
dtr.AirTempFP3 AS AirTempFP3,
dtr.RainfallFP3 AS RainfallFP3,
dtr.TrackTempFP3 AS TrackTempFP3,
dtr.WindDirectionFP3 AS WindDirectionFP3,
dtr.WindSpeedFP3 AS WindSpeedFP3,
dtr.LapTimeQ AS LapTimeQ,
dtr.AirTempQ AS AirTempQ,
dtr.RainfallQ AS RainfallQ,
dtr.WindDirectionQ AS WindDirectionQ,
dtr.WindSpeedQ AS WindSpeedQ,
(CASE
    WHEN (dtr.event_type = 'conventional') THEN 0
    ELSE 1
END) AS sprint_flag,
trackdtr.traction AS traction,
trackdtr.tyre_stress AS tyre_stress,
trackdtr.asphalt_grip AS asphalt_grip,
trackdtr.braking AS braking,
trackdtr.asphalt_abrasion AS asphalt_abrasion,
trackdtr.lateral_force AS lateral_force,
trackdtr.track_evolution AS track_evolution,
trackdtr.downforce AS downforce
FROM
(((((raw_session_data dtr
LEFT JOIN dim_compound fp1comp ON ((dtr.CompoundFP1 = fp1comp.compound_name)))
LEFT JOIN dim_compound fp2comp ON ((dtr.CompoundFP2 = fp2comp.compound_name)))
LEFT JOIN dim_compound fp3comp ON ((dtr.CompoundFP3 = fp3comp.compound_name)))
LEFT JOIN dim_event evt ON ((dtr.event_name = evt.event_name)))
LEFT JOIN track_data trackdtr ON ((evt.event_cd = trackdtr.event_cd)))
WHERE
(dtr.LapTimeQ <> 0)