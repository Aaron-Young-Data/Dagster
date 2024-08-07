SELECT
        IFNULL(DAT.LAPTIME_FP1, 0) AS LAPTIME_FP1,
        IFNULL(DAT.COMPOUND_FP1, 0) AS COMPOUND_FP1,
        IFNULL(DAT.AIR_TEMP_FP1, 0) AS AIR_TEMP_FP1,
        IFNULL(DAT.RAINFALL_FP1, 0) AS RAINFALL_FP1,
        IFNULL(DAT.TRACK_TEMP_FP1, 0) AS TRACK_TEMP_FP1,
        CASE WHEN isnull(DAT.LAPTIME_FP1) THEN 1 ELSE 0 END AS FP1_MISSING_FLAG,
        IFNULL(DAT.LAPTIME_FP2, 0) AS LAPTIME_FP2,
        IFNULL(DAT.COMPOUND_FP2, 0) AS COMPOUND_FP2,
        IFNULL(DAT.AIR_TEMP_FP2, 0) AS AIR_TEMP_FP2,
        IFNULL(DAT.RAINFALL_FP2, 0) AS RAINFALL_FP2,
        IFNULL(DAT.TRACK_TEMP_FP2, 0) AS TRACK_TEMP_FP2,
        CASE WHEN EVT.EVENT_YEAR <= 2022 THEN CASE WHEN isnull(DAT.LAPTIME_FP2) THEN 1 ELSE 0 END ELSE 0 END FP2_MISSING_FLAG,
        IFNULL(DAT.LAPTIME_FP3, 0) AS LAPTIME_FP3,
        IFNULL(DAT.COMPOUND_FP3, 0) AS COMPOUND_FP3,
        IFNULL(DAT.AIR_TEMP_FP3, 0) AS AIR_TEMP_FP3,
        IFNULL(DAT.RAINFALL_FP3, 0) AS RAINFALL_FP3,
        IFNULL(DAT.TRACK_TEMP_FP3, 0) AS TRACK_TEMP_FP3,
        0 AS FP3_MISSING_FLAG,
        DAT.LAPTIME_Q,
        DAT.AIR_TEMP_Q,
        DAT.RAINFALL_Q,
        CASE WHEN EVT.EVENT_TYPE_CD = 2 THEN 1 ELSE 0 END AS SPRINT_FLAG,
        TRK.TRACTION,
        TRK.TYRE_STRESS,
        TRK.ASPHALT_GRIP,
        TRK.BRAKING,
        TRK.ASPHALT_ABRASION,
        TRK.LATERAL_FORCE,
        TRK.TRACK_EVOLUTION,
        TRK.DOWNFORCE
    FROM CLEANED_SESSION_DATA DAT
    LEFT JOIN DIM_EVENT EVT
        ON DAT.EVENT_CD = EVT.EVENT_CD
    LEFT JOIN DIM_TRACK TRK
        ON TRK.TRACK_CD = EVT.TRACK_CD
    WHERE
        EVT.EVENT_TYPE_CD = 2 and
        DAT.LAPTIME_Q is not NULL
    UNION ALL
    SELECT
        IFNULL(DAT.LAPTIME_FP1, 0) AS LAPTIME_FP1,
        IFNULL(DAT.COMPOUND_FP1, 0) AS COMPOUND_FP1,
        IFNULL(DAT.AIR_TEMP_FP1, 0) AS AIR_TEMP_FP1,
        IFNULL(DAT.RAINFALL_FP1, 0) AS RAINFALL_FP1,
        IFNULL(DAT.TRACK_TEMP_FP1, 0) AS TRACK_TEMP_FP1,
        CASE WHEN isnull(DAT.LAPTIME_FP1) THEN 1 ELSE 0 END AS FP1_MISSING_FLAG,
        IFNULL(DAT.LAPTIME_FP2, 0) AS LAPTIME_FP2,
        IFNULL(DAT.COMPOUND_FP2, 0) AS COMPOUND_FP2,
        IFNULL(DAT.AIR_TEMP_FP2, 0) AS AIR_TEMP_FP2,
        IFNULL(DAT.RAINFALL_FP2, 0) AS RAINFALL_FP2,
        IFNULL(DAT.TRACK_TEMP_FP2, 0) AS TRACK_TEMP_FP2,
        CASE WHEN isnull(DAT.LAPTIME_FP2) THEN 1 ELSE 0  END FP2_MISSING_FLAG,
        IFNULL(DAT.LAPTIME_FP3, 0) AS LAPTIME_FP3,
        IFNULL(DAT.COMPOUND_FP3, 0) AS COMPOUND_FP3,
        IFNULL(DAT.AIR_TEMP_FP3, 0) AS AIR_TEMP_FP3,
        IFNULL(DAT.RAINFALL_FP3, 0) AS RAINFALL_FP3,
        IFNULL(DAT.TRACK_TEMP_FP3, 0) AS TRACK_TEMP_FP3,
        CASE WHEN isnull(DAT.LAPTIME_FP3) THEN 1 ELSE 0 END AS FP3_MISSING_FLAG,
        DAT.LAPTIME_Q,
        DAT.AIR_TEMP_Q,
        DAT.RAINFALL_Q,
        CASE WHEN EVT.EVENT_TYPE_CD = 2 THEN 1 ELSE 0 END AS SPRINT_FLAG,
        TRK.TRACTION,
        TRK.TYRE_STRESS,
        TRK.ASPHALT_GRIP,
        TRK.BRAKING,
        TRK.ASPHALT_ABRASION,
        TRK.LATERAL_FORCE,
        TRK.TRACK_EVOLUTION,
        TRK.DOWNFORCE
    FROM CLEANED_SESSION_DATA DAT
    LEFT JOIN DIM_EVENT EVT
        ON DAT.EVENT_CD = EVT.EVENT_CD
    LEFT JOIN DIM_TRACK TRK
        ON TRK.TRACK_CD = EVT.TRACK_CD
    WHERE
        EVT.EVENT_TYPE_CD = 1 and
        DAT.LAPTIME_Q is not NULL