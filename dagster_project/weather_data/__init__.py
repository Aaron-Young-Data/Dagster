import os
from dagster import (Definitions, ResourceDefinition)
from dagster_mysql import MySQLResource
from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager, jolpi_api, fast_f1_resource

all_assets = [*dim_table_update_assets,
              *weather_data_update_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        weather_forecast_data_load_job,
        weather_type_load_job,
    ],
    schedules=[
        weather_forecast_schedule,
        update_weather_data_type_schedule
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
