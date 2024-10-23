CREATE OR REPLACE VIEW TABLEAU_DATA.AGG_RACE_RESULTS_DRIVER_VW AS
(
WITH ACCURATE_LAPS AS (SELECT EVT.EVENT_CD,
                              DAT.*
                       FROM TABLEAU_DATA.ALL_SESSION_DATA DAT
                                LEFT JOIN ML_PROJECT_PROD.DIM_EVENT EVT
                                          ON DAT.EVENT_NAME = EVT.EVENT_NAME AND DAT.YEAR = EVT.EVENT_YEAR
                       WHERE IS_ACCURATE = 1
                         AND SESSION IN ('Race', 'Sprint')),

     COMPLETED_LAPS AS (SELECT CONCAT(YEAR, EVENT_NAME, DRIVER, DRIVER_NUMBER, SESSION) AS ID_DRIVER,
                               MAX(LAP_NUMBER)                                 AS COMPLETED_LAPS,
                               MIN(LAP_TIME)                                   AS DRIVER_FASTEST_LAP
                        FROM ACCURATE_LAPS
                        GROUP BY YEAR, EVENT_NAME, DRIVER, DRIVER_NUMBER, SESSION),

     LAPS_RACE AS (SELECT CONCAT(YEAR, EVENT_NAME, SESSION) AS ID_EVENT,
                          MAX(LAP_NUMBER)          AS RACE_LAPS,
                          MIN(LAP_TIME)            AS RACE_FASTEST_LAP
                   FROM ACCURATE_LAPS
                   GROUP BY YEAR, EVENT_NAME, SESSION),

     FINAL AS (SELECT DAT.EVENT_CD,
                      DAT.SESSION,
                      DAT.DRIVER AS DRIVER,
                      DAT.TEAM,
                      LAPS.COMPLETED_LAPS,
                      CASE
                          WHEN
                              LAPS.COMPLETED_LAPS = DAT.LAP_NUMBER
                              THEN
                              CASE WHEN DAT.SESSION = 'Sprint'
                                  THEN CASE
                                  WHEN
                                      R_LAPS.RACE_LAPS - 1 <= DAT.LAP_NUMBER
                                      THEN
                                      DAT.POSITION
                                  ELSE
                                      'DNF'
                                  END
                              ELSE
                                  CASE
                                  WHEN
                                      R_LAPS.RACE_LAPS - 3 <= DAT.LAP_NUMBER
                                      THEN
                                      DAT.POSITION
                                  ELSE
                                      'DNF'
                                  END
                              END
                          END                                                                       AS FINAL_POSITION,
                      ROUND(R_LAPS.RACE_FASTEST_LAP, 3)                                             AS RACE_FASTEST_LAP,
                      ROUND(LAPS.DRIVER_FASTEST_LAP, 3)                                             AS DRIVER_FASTEST_LAP,
                      CASE WHEN LAPS.DRIVER_FASTEST_LAP = R_LAPS.RACE_FASTEST_LAP THEN 1 ELSE 0 END AS FASTEST_LAP_FLAG,
                      MAX(STINT) - 1                                                                AS NUM_PITSTOPS
               FROM ACCURATE_LAPS DAT
                        LEFT JOIN COMPLETED_LAPS LAPS
                                  ON CONCAT(DAT.YEAR, DAT.EVENT_NAME, DAT.DRIVER, DAT.DRIVER_NUMBER, DAT.SESSION) = LAPS.ID_DRIVER
                        LEFT JOIN LAPS_RACE R_LAPS
                                  ON CONCAT(DAT.YEAR, DAT.EVENT_NAME, DAT.SESSION) = R_LAPS.ID_EVENT
               GROUP BY DAT.EVENT_CD,
                        DAT.SESSION,
                        DAT.DRIVER,
                        DAT.DRIVER_NUMBER,
                        DAT.TEAM,
                        LAPS.COMPLETED_LAPS,
                        FINAL_POSITION,
                        R_LAPS.RACE_FASTEST_LAP,
                        LAPS.DRIVER_FASTEST_LAP,
                        DAT.STINT)

SELECT DAT.EVENT_CD,
       DAT.SESSION,
       DAT.DRIVER,
       DAT.TEAM,
       DAT.FINAL_POSITION,
       IFNULL(PNT.POSITION_POINTS, 0) +
       CASE
           WHEN FASTEST_LAP_POINT_FLAG = TRUE
               THEN
               CASE
                   WHEN FASTEST_LAP_FLAG = 1
                       THEN 1
                   ELSE 0
                   END
           ELSE 0
           END AS POINTS_SCORED,
       DAT.COMPLETED_LAPS,
       DAT.NUM_PITSTOPS,
       DAT.DRIVER_FASTEST_LAP,
       DAT.FASTEST_LAP_FLAG
FROM FINAL DAT
         LEFT JOIN TABLEAU_DATA.DIM_POINTS PNT
                   ON PNT.YEAR = LEFT(DAT.EVENT_CD, 4)
                       AND DAT.FINAL_POSITION = PNT.DRIVER_POSITION
                       AND UPPER(DAT.SESSION) = PNT.RACE_TYPE
WHERE FINAL_POSITION IS NOT NULL
ORDER BY CAST(LEFT(EVENT_CD, 4) AS DECIMAL),
         CAST(RIGHT(EVENT_CD, LENGTH(EVENT_CD) - 4) AS DECIMAL),
         COMPLETED_LAPS DESC,
         CAST((CASE WHEN FINAL_POSITION = 'DNF' THEN 100 ELSE FINAL_POSITION END) AS DECIMAL)
    )