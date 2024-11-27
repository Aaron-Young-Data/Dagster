DROP TABLE IF EXISTS PREDICTION_DATA;

CREATE TABLE PREDICTION_DATA (
    EVENT_CD INT(2),
    DRIVER VARCHAR(3),
    DRIVER_NUMBER INT(2),
    PREDICTED_LAPTIME_Q FLOAT,
    LOAD_TS TIMESTAMP
);