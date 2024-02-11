SELECT
IFNULL(dtr.LapTimeFP1, 0) as LapTimeFP1,
IFNULL(dtr.Sector1TimeFP1, 0) as Sector1TimeFP1,
IFNULL(dtr.Sector2TimeFP1, 0) as Sector2TimeFP1,
IFNULL(dtr.Sector3TimeFP1, 0) as Sector3TimeFP1,
IFNULL(fp1comp.compount_cd, 0) as CompoundFP1,
IFNULL(dtr.AirTempFP1, 0) as AirTempFP1,
CASE WHEN dtr.RainfallFP1 = 'FALSE' THEN 0 ELSE '1' END as RainfallFP1,
IFNULL(dtr.TrackTempFP1, 0) as TrackTempFP1,
IFNULL(dtr.WindDirectionFP1, 0) as WindDirectionFP1,
IFNULL(dtr.WindSpeedFP1, 0) as WindSpeedFP1,
IFNULL(dtr.LapTimeFP2, 0) as LapTimeFP2,
IFNULL(dtr.Sector1TimeFP2, 0) as Sector1TimeFP2,
IFNULL(dtr.Sector2TimeFP2, 0) as Sector2TimeFP2,
IFNULL(dtr.Sector3TimeFP2, 0) as Sector3TimeFP2,
IFNULL(fp2comp.compount_cd, 0) as CompoundFP2,
IFNULL(dtr.AirTempFP2, 0) as AirTempFP2,
CASE WHEN dtr.RainfallFP2 = 'FALSE' THEN 0 ELSE '1' END as RainfallFP2,
IFNULL(dtr.TrackTempFP2, 0) as TrackTempFP2,
IFNULL(dtr.WindDirectionFP2, 0) as WindDirectionFP2,
IFNULL(dtr.WindSpeedFP2, 0) as WindSpeedFP2,
IFNULL(dtr.LapTimeFP3, 0) as LapTimeFP3,
IFNULL(dtr.Sector1TimeFP3, 0) as Sector1TimeFP3,
IFNULL(dtr.Sector2TimeFP3, 0) as Sector2TimeFP3,
IFNULL(dtr.Sector3TimeFP3, 0) as Sector3TimeFP3,
IFNULL(fp3comp.compount_cd, 0) as CompoundFP3,
IFNULL(dtr.AirTempFP3, 0) as AirTempFP3,
CASE WHEN dtr.RainfallFP3 = 'FALSE' THEN 0 ELSE '1' END as RainfallFP3,
IFNULL(dtr.TrackTempFP3, 0) as TrackTempFP3,
IFNULL(dtr.WindDirectionFP3, 0) as WindDirectionFP3,
IFNULL(dtr.WindSpeedFP3, 0) as WindSpeedFP3,
IFNULL(dtr.LapTimeQ, 0) as LapTimeQ,
CASE WHEN dtr.event_type = 'conventional' THEN 0 ELSE 1 END as sprint_flag,
trackdtr.traction,
trackdtr.tyre_stress,
trackdtr.asphalt_grip,
trackdtr.braking,
trackdtr.asphalt_abrasion,
trackdtr.lateral_force,
trackdtr.track_evolution,
trackdtr.downforce
FROM
ml_project_dev.raw_session_data dtr
LEFT JOIN ml_project_dev.dim_compound fp1comp
on dtr.compoundFP1 = fp1comp.compound_name
LEFT JOIN ml_project_dev.dim_compound fp2comp
on dtr.compoundFP2 = fp2comp.compound_name
LEFT JOIN ml_project_dev.dim_compound fp3comp
on dtr.compoundFP3 = fp3comp.compound_name
LEFT JOIN ml_project_dev.dim_track a
on dtr.event_name = a.event_name
LEFT JOIN ml_project_dev.track_data trackdtr
on a.event_cd = trackdtr.event_cd
WHERE dtr.LapTimeQ != 0
