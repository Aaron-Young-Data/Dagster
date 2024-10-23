CREATE OR REPLACE VIEW AGG_DRIVERS_CHAMPIONSHIP_RESULTS AS
(
WITH DRIVERS_POINTS AS (SELECT LEFT(EVENT_CD, 4)                                                                         AS YEAR,
                               DRIVER,
                               SUM(POINTS_SCORED)                                                                        AS TOTAL_POINTS,
                               MIN(CAST((CASE WHEN FINAL_POSITION = 'DNF' OR SESSION = 'Sprint' THEN 100 ELSE FINAL_POSITION END) AS DECIMAL)) AS BEST_FINISH
                        FROM TABLEAU_DATA.AGG_RACE_RESULTS_DRIVER_VW
                        GROUP BY YEAR,
                                 DRIVER
                        ORDER BY YEAR,
                                 TOTAL_POINTS DESC)

SELECT YEAR,
       DRIVER,
       TOTAL_POINTS,
       DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY TOTAL_POINTS DESC, BEST_FINISH) AS CHAMPIONSHIP_POS,
       MIN(BEST_FINISH) AS SEASON_BEST_FINISH
FROM DRIVERS_POINTS
GROUP BY YEAR,
         DRIVER,
         TOTAL_POINTS
ORDER BY YEAR,
         CHAMPIONSHIP_POS
    )