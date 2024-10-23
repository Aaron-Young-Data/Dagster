import os
from dagster import (Definitions, ResourceDefinition)
from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager

all_assets = [*table_assets,
              *dim_table_assets,
              *view_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_dim_track_status_table_job,
        create_all_session_data_table_job,
        create_race_results_view_job,
        create_qualifying_results_view_job,
        create_practice_results_view_job,
        create_constructors_championship_view_job,
        create_drivers_championship_view_job
    ],
    schedules=[],
    sensors=[],
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
