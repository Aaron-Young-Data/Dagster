from dagster import (AssetSelection,
                     Definitions,
                     define_asset_job,
                     ScheduleDefinition,
                     build_schedule_from_partitioned_job,
                     EnvVar)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *

all_assets = [*testing_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        create_prediction_job
        ],
    schedules=[],
    sensors=[],
)
