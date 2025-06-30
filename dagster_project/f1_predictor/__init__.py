from dagster import (Definitions, ResourceDefinition)
from dagster_mysql import MySQLResource
from resources import sql_io_manager, jolpi_api, fast_f1_resource

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *

import os

all_assets = [*f1_predictor_assets,
              *f1_predictor_evaluation_assets,
              *pre_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_qualifying_prediction_job,
        evaluate_qualifying_prediction_job
    ],
    schedules=[],
    sensors=[
        create_qualifying_prediction_job_sensor,
        evaluate_qualifying_prediction_job_sensor
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
