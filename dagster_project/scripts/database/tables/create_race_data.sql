DROP TABLE IF EXISTS PREDICTION.RACE_PREDICTION_DATA;

CREATE TABLE PREDICTION.RACE_PREDICTION_DATA (
    EVENT_CD INT(6),
    DRIVER_ID VARCHAR(50),
    PREDICTED_POSITION INT,
    LOAD_TS TIMESTAMP
);