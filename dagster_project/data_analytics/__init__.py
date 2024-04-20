import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager

all_assets = [*data_load_assets,
              *data_download_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[download_all_session_data_job,
          data_load_track_status_data_job,
          data_load_weekend_session_data
          ],
    schedules=[update_track_status_data_job_weekly_schedule],
    sensors=[weekend_session_data_load_job_sensor,
             session_data_download_job_sensor],
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
