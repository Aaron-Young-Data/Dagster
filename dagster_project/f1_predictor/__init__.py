import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager

all_assets = [*f1_predictor_assets,
              *f1_predictor_evaluation_assets,
              *f1_dnn_model_creation_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job,
        evaluate_prediction_job,
        create_dnn_model_job
    ],
    schedules=[],
    sensors=[create_prediction_job_sensor,
             evaluate_prediction_job_sensor,
             create_dnn_model_discord_sensor,
             create_dnn_model_sensor],
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
