import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from .resources import sql_io_manager

all_assets = [*f1_predictor_assets, *data_update_assets, *database_build_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job,
        update_calender_job,
        session_data_load_job,
        track_data_load_job,
        compound_data_load_job
    ],
    schedules=[update_calender_job_weekly_schedule],
    sensors=[create_prediction_job_sensor],
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
