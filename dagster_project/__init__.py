import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from .resources import sql_io_manager

all_assets = [*f1_predictor_assets, *data_update_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job,
        update_calender_job,
        session_data_load_job,
        track_data_load_job,
        session_data_clean_job
    ],
    schedules=[update_calender_job_weekly_schedule],
    sensors=[create_prediction_job_sensor],
    resources={
        'sql_io_manager_dev': sql_io_manager.SQLIOManager(
            user=os.getenv('user'),
            password=os.getenv('password'),
            database=os.getenv('database_dev'),
            port=os.getenv('port'),
            server=os.getenv('server'),
        ),
    },
)
