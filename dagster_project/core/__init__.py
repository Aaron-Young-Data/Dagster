import os

from dagster import (Definitions, ResourceDefinition)

from .assets import *
from .jobs import *
from .schedules import *
from .sensors import *
from resources import sql_io_manager

all_assets = [*core_database_assets]

defs = Definitions(
    assets=all_assets,
    jobs=[
        mysql_daily_backup_job,
        mysql_restore_job
    ],
    schedules=[
        mysql_daily_backup_schedule
    ],
    sensors=[discord_failure_sensor],
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
