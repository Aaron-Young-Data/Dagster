import pandas as pd
from dagster import (sensor,
                     RunRequest,
                     SkipReason,
                     DagsterRunStatus,
                     RunsFilter,
                     RunFailureSensorContext,
                     run_failure_sensor)
from .jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os
from resources.sql_io_manager import MySQLDirectConnection
from utils.discord_utils import DiscordUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')
tableau_data_loc = os.getenv('TABLEAU_DATA_LOC')


@run_failure_sensor(monitor_all_code_locations=True)
def discord_failure_sensor(context: RunFailureSensorContext):
    dis = DiscordUtils()
    dis.send_message(message=f'Job: {context.dagster_run.job_name} failed!\n'
                             f'Error: {context.failure_event.message}',
                     channel_id='1231256334511636521')
