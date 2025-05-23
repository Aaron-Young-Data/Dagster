CREATE OR REPLACE VIEW TABLEAU_DATA.AGG_QUALIFYING_RESULTS_VW AS (
WITH ACCURATE_LAPS AS (
    SELECT
        EVT.EVENT_CD,
        CASE WHEN SESSION IN ('Sprint Shootout', 'Sprint Qualifying')
            THEN 'Sprint Qualifying'
            ELSE 'Qualifying'
        END AS SESSION,
        DAT.DRIVER,
        DAT.TEAM,
        DAT.LAP_TIME,
        DAT.COMPOUND
    FROM TABLEAU_DATA.ALL_SESSION_DATA DAT
    LEFT JOIN ML_PROJECT_PROD.DIM_EVENT EVT
        ON DAT.EVENT_NAME = EVT.EVENT_NAME AND DAT.YEAR = EVT.EVENT_YEAR
    WHERE IS_ACCURATE = 1
    AND SESSION IN ('Qualifying', 'Sprint Shootout', 'Sprint Qualifying')
),

FASTEST_LAPS AS (
SELECT
    CONCAT(EVENT_CD, SESSION, DRIVER) AS DRIVER_ID,
    MIN(LAP_TIME) AS FASTEST_LAP
FROM ACCURATE_LAPS
GROUP BY
    EVENT_CD,
    SESSION,
    DRIVER
)

SELECT
    DAT.EVENT_CD,
    DAT.SESSION,
    DAT.DRIVER,
    DAT.TEAM,
    DAT.LAP_TIME,
    DENSE_RANK() OVER (PARTITION BY EVENT_CD, SESSION ORDER BY LAPS.FASTEST_LAP) AS QUALIFYING_POS,
    CASE WHEN DAT.LAP_TIME = LAPS.FASTEST_LAP THEN DAT.COMPOUND END AS COMPOUND
FROM ACCURATE_LAPS DAT
LEFT JOIN FASTEST_LAPS LAPS
    ON CONCAT(DAT.EVENT_CD, DAT.SESSION, DAT.DRIVER) = LAPS.DRIVER_ID
WHERE
    CASE WHEN DAT.LAP_TIME = LAPS.FASTEST_LAP THEN DAT.COMPOUND END IS NOT NULL
GROUP BY
    DAT.EVENT_CD,
    DAT.SESSION,
    DAT.DRIVER,
    DAT.TEAM,
    LAPS.FASTEST_LAP,
    DAT.LAP_TIME,
    7
ORDER BY
    SESSION,
    CAST(LEFT(EVENT_CD, 4) AS DECIMAL),
    CAST(RIGHT(EVENT_CD, LENGTH(EVENT_CD) - 4) AS DECIMAL),
    LAP_TIME,
    QUALIFYING_POS)