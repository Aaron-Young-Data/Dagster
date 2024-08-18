import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager

all_assets = [*dim_table_update_assets,
              *session_data_update_assets,
              *weather_data_update_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        update_calender_job,
        full_session_data_load_job,
        track_data_load_job,
        compound_data_load_job,
        session_data_load_job,
        weather_forecast_data_load_job,
        location_data_load_job,
        weather_type_load_job
    ],
    schedules=[update_calender_job_weekly_schedule,
               update_compound_job_weekly_schedule,
               update_track_job_weekly_schedule,
               weather_forecast_schedule,
               update_location_job_weekly_schedule,
               update_weather_data_type_schedule],
    sensors=[
             full_session_data_load_job_sensor,
             session_data_load_job_sensor],
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
