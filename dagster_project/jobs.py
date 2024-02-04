from dagster import (
    AssetSelection,
    define_asset_job,
    ScheduleDefinition)

from .assets import *

create_prediction_job = define_asset_job("F1_prediction_job",
                                         selection=AssetSelection.groups(F1_PREDICTOR),
                                         description="Job to predict the f1 qualifying and output to discord")