import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from .resources import sql_io_manager

all_assets = [*f1_predictor_assets,
              *data_update_assets,
              *database_build_assets,
              *f1_predictor_evaluation_assets,
              *data_analysis_load_assets,
              *data_analysis_download_assets,
              *data_analysis_database_build]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job,
        update_calender_job,
        session_data_load_job,
        track_data_load_job,
        compound_data_load_job,
        evaluate_prediction_job,
        weekend_session_data_load_job,
        weather_forecast_data_load_job,
        load_data_analysis_data_job,
        download_all_session_data_job_analytics,
        load_track_status_data_job_analytics,
        load_weekend_session_data_analytics
    ],
    schedules=[update_calender_job_weekly_schedule,
               update_compound_job_weekly_schedule,
               update_track_job_weekly_schedule,
               weather_forcast_schedule,
               update_track_status_data_job_weekly_schedule],
    sensors=[create_prediction_job_sensor,
             evaluate_prediction_job_sensor,
             weekend_session_data_load_job_sensor,
             session_data_load_job_sensor,
             analytics_session_data_load_job_sensor,
             analytics_session_data_download_job_sensor,
             analytics_weekend_session_data_load_job_sensor],
    resources={
        'sql_io_manager': sql_io_manager.SQLIOManager(
            user=os.getenv('SQL_USER'),
            password=os.getenv('SQL_PASSWORD'),
            database=os.getenv('DATABASE'),
            port=os.getenv('SQL_PORT'),
            server=os.getenv('SQL_SERVER'),
        ),
    },
)
