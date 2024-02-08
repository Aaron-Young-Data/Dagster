from dagster import (Definitions)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *

all_assets = [*f1_predictor_assets, *calender_update_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job,
        update_calender_job
        ],
    schedules=[update_calender_job_weekly_schedule],
    sensors=[create_prediction_job_sensor],
)
