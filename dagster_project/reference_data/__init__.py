import os
from dagster import (Definitions, ResourceDefinition)
from dagster_mysql import MySQLResource
from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager, jolpi_api, fast_f1_resource

all_assets = [*api_update_assets, *file_update_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        update_calender_job,
        track_data_load_job,
        track_event_data_load_job,
        compound_data_load_job,
        update_driver_jobs,
        update_constructors_jobs,
        update_dim_session_job
    ],
    schedules=[
        update_calender_job_weekly_schedule,
        update_compound_job_weekly_schedule,
        update_track_job_weekly_schedule,
        update_track_event_job_weekly_schedule,
        update_driver_data_schedule,
        update_constructor_data_schedule,
        update_dim_session_data_schedule
    ],
    sensors=[
    ],
    resources={
        'sql_io_manager': sql_io_manager.SQLIOManager(
            user=os.getenv('SQL_USER'),
            password=os.getenv('SQL_PASSWORD'),
            database=os.getenv('DATABASE'),
            port=os.getenv('SQL_PORT'),
            server=os.getenv('SQL_SERVER'),
        ),
        'mysql': MySQLResource(
            user=os.getenv('SQL_USER'),
            password=os.getenv('SQL_PASSWORD'),
            port=os.getenv('SQL_PORT'),
            host=os.getenv('SQL_SERVER'),
        ),
        'fastf1': fast_f1_resource.FastF1Resource(
            cache_loc=os.getenv('FAST_F1_CACHE_LOC')
        ),
        'jolpi_api': jolpi_api.JolpiResource(

        ),
    },
)
