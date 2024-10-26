CREATE OR REPLACE VIEW TABLEAU_DATA.AGG_PRACTICE_RESULTS_VW AS (
WITH ACCURATE_LAPS AS (
    SELECT
        EVT.EVENT_CD,
        DAT.SESSION,
        DAT.DRIVER,
        DAT.TEAM,
        DAT.LAP_TIME,
        DAT.COMPOUND
    FROM TABLEAU_DATA.ALL_SESSION_DATA DAT
    LEFT JOIN ML_PROJECT_PROD.DIM_EVENT EVT
        ON DAT.EVENT_NAME = EVT.EVENT_NAME AND DAT.YEAR = EVT.EVENT_YEAR
    WHERE IS_ACCURATE = 1
    AND SESSION IN ('Practice 1', 'Practice 2', 'Practice 3')
),

FASTEST_LAPS AS (
SELECT
    EVENT_CD,
    SESSION,
    DRIVER,
    TEAM,
    MIN(LAP_TIME) AS FASTEST_LAP
FROM ACCURATE_LAPS
GROUP BY
    EVENT_CD,
    SESSION,
    DRIVER,
    TEAM
)

SELECT
    DAT.EVENT_CD,
    DAT.SESSION,
    DAT.DRIVER,
    DAT.TEAM,
    ROUND(LAP.FASTEST_LAP, 3) AS LAP_TIME,
    DENSE_RANK() OVER (PARTITION BY DAT.EVENT_CD, DAT.SESSION ORDER BY FASTEST_LAP ) AS QUALIFYING_POS,
    CASE WHEN LAP.FASTEST_LAP = DAT.LAP_TIME THEN DAT.COMPOUND END AS COMPOUND
FROM ACCURATE_LAPS DAT
LEFT JOIN FASTEST_LAPS LAP
    ON LAP.EVENT_CD = DAT.EVENT_CD
    AND LAP.SESSION = DAT.SESSION
    AND LAP.DRIVER = DAT.DRIVER
WHERE
    CASE WHEN LAP.FASTEST_LAP = DAT.LAP_TIME THEN DAT.COMPOUND END IS NOT NULL
GROUP BY
    DAT.EVENT_CD,
    DAT.SESSION,
    DAT.DRIVER,
    DAT.TEAM,
    LAP.FASTEST_LAP,
    DAT.LAP_TIME,
    7
ORDER BY
    CAST(LEFT(DAT.EVENT_CD, 4) AS DECIMAL),
    CAST(RIGHT(DAT.EVENT_CD, LENGTH(DAT.EVENT_CD) - 4) AS DECIMAL),
    LAP_TIME
)