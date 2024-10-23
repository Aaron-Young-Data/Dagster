DROP VIEW IF EXISTS DIM_EVENT;

CREATE VIEW DIM_EVENT AS (
    SELECT 
        CONCAT(YEAR(cldr.EventDate), cldr.RoundNumber) AS EVENT_CD,
        cldr.RoundNumber AS ROUND_NUMBER,
        cldr.EventDate AS EVENT_DT,
        YEAR(cldr.EventDate) AS EVENT_YEAR,
        cldr.EventName AS EVENT_NAME,
        cldr.Location AS LOCATION,
        CASE
            WHEN (cldr.Location LIKE '%Montr%') THEN 'Montreal'
            WHEN (cldr.Location LIKE '%Paulo%') THEN 'SaoPaulo'
            WHEN (cldr.Location LIKE '%Portim%') THEN 'Portimao'
            WHEN (cldr.Location LIKE '%rburgring%') THEN 'Nurburgring'
            WHEN (cldr.Location = 'Baku') THEN 'Azerbaijan'
            WHEN (cldr.Location = 'Marina Bay') THEN 'Singapore'
            WHEN (cldr.Location = 'Yas Marina') THEN 'YasIsland'
            ELSE REPLACE(cldr.Location, ' ', '')
        END AS FCST_LOCATION,
        trk.TRACK_CD AS TRACK_CD,
        cldr.EventFormat AS EVENT_TYPE,
        CASE
            WHEN (cldr.EventFormat = 'conventional') THEN 1
            WHEN (cldr.EventFormat LIKE '%sprint%') THEN 2
            ELSE -(2)
        END AS EVENT_TYPE_CD,
        cldr.Session1 AS SESSION_ONE_TYPE,
        cldr.Session1DateUtc AS SESSION_ONE_DT,
        cldr.Session2 AS SESSION_TWO_TYPE,
        cldr.Session2DateUtc AS SESSION_TWO_DT,
        cldr.Session3 AS SESSION_THREE_TYPE,
        cldr.Session3DateUtc AS SESSION_THREE_DT,
        cldr.Session4 AS SESSION_FOUR_TYPE,
        cldr.Session4DateUtc AS SESSION_FOUR_DT,
        cldr.Session5 AS SESSION_FIVE_TYPE,
        cldr.Session5DateUtc AS SESSION_FIVE_DT
    FROM F1_CALENDER cldr
    LEFT JOIN DIM_TRACK_EVENT trk
        ON CONCAT(YEAR(cldr.EventDate), cldr.RoundNumber) = trk.EVENT_CD
)