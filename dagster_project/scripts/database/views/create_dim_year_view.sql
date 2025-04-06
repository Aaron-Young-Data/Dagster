CREATE OR REPLACE VIEW DIM_YEAR AS (
    SELECT
        DISTINCT
        EVENT_YEAR AS YEAR,
        EVENT_YEAR-1 AS YEAR_LY,
        EVENT_YEAR-2 AS YEAR_LLY
    FROM DIM_EVENT
    ORDER BY
        EVENT_YEAR DESC
)