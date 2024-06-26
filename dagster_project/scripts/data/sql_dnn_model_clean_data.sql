SELECT
        IFNULL(LapTimeFP1, 0) AS LapTimeFP1,
        IFNULL(CompoundFP1, 0) AS CompoundFP1,
        IFNULL(AirTempFP1, 0) AS AirTempFP1,
        IFNULL(RainfallFP1, 0) AS RainfallFP1,
        IFNULL(TrackTempFP1, 0) AS TrackTempFP1,
        CASE WHEN isnull(LapTimeFP1) THEN 1 ELSE 0 END AS FP1_Missing_Flag,
        IFNULL(LapTimeFP2, 0) AS LapTimeFP2,
        IFNULL(CompoundFP2, 0) AS CompoundFP2,
        IFNULL(AirTempFP2, 0) AS AirTempFP2,
        IFNULL(RainfallFP2, 0) AS RainfallFP2,
        IFNULL(TrackTempFP2, 0) AS TrackTempFP2,
        CASE WHEN year <= 2022 THEN CASE WHEN isnull(LapTimeFP2) THEN 1 ELSE 0 END ELSE 0 END FP2_Missing_Flag,
        IFNULL(LapTimeFP3, 0) AS LapTimeFP3,
        IFNULL(CompoundFP3, 0) AS CompoundFP3,
        IFNULL(AirTempFP3, 0) AS AirTempFP3,
        IFNULL(RainfallFP3, 0) AS RainfallFP3,
        IFNULL(TrackTempFP3, 0) AS TrackTempFP3,
        0 AS FP3_Missing_Flag,
        LapTimeQ,
        AirTempQ,
        RainfallQ,
        sprint_flag as is_sprint,
        traction,
        tyre_stress,
        asphalt_grip,
        braking,
        asphalt_abrasion,
        lateral_force,
        track_evolution,
        downforce
    FROM cleaned_session_data
    where sprint_flag = 1 and
    LapTimeQ is not NULL
    UNION ALL
    SELECT
        IFNULL(LapTimeFP1, 0) AS LapTimeFP1,
        IFNULL(CompoundFP1, 0) AS CompoundFP1,
        IFNULL(AirTempFP1, 0) AS AirTempFP1,
        IFNULL(RainfallFP1, 0) AS RainfallFP1,
        IFNULL(TrackTempFP1, 0) AS TrackTempFP1,
        CASE WHEN isnull(LapTimeFP1) THEN 1 ELSE 0 END AS FP1_Missing_Flag,
        IFNULL(LapTimeFP2, 0) AS LapTimeFP2,
        IFNULL(CompoundFP2, 0) AS CompoundFP2,
        IFNULL(AirTempFP2, 0) AS AirTempFP2,
        IFNULL(RainfallFP2, 0) AS RainfallFP2,
        IFNULL(TrackTempFP2, 0) AS TrackTempFP2,
        CASE WHEN isnull(LapTimeFP2) THEN 1 ELSE 0 END AS FP2_Missing_Flag,
        IFNULL(LapTimeFP3, 0) AS LapTimeFP3,
        IFNULL(CompoundFP3, 0) AS CompoundFP3,
        IFNULL(AirTempFP3, 0) AS AirTempFP3,
        IFNULL(RainfallFP3, 0) AS RainfallFP3,
        IFNULL(TrackTempFP3, 0) AS TrackTempFP3,
        CASE WHEN isnull(LapTimeFP3) THEN 1 ELSE 0 END AS FP3_Missing_Flag,
        LapTimeQ,
        AirTempQ,
        RainfallQ,
        sprint_flag as is_sprint,
        traction,
        tyre_stress,
        asphalt_grip,
        braking,
        asphalt_abrasion,
        lateral_force,
        track_evolution,
        downforce
    FROM cleaned_session_data
    where sprint_flag = 0 and
    LapTimeQ is not NULL