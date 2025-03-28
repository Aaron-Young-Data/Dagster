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
        create_weather_forcast_table_job,
        create_dim_track_table_job,
        create_dim_track_event_table_job,
        create_session_data_table_job,
        create_f1_calender_table_job,
        create_dim_event_view_job,
        create_dim_compound_table_job,
        create_weather_forcast_view_job,
        create_dim_weather_type_table_job,
        create_cleaned_session_data_view_job,
        create_prediction_data_table_job
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
