import pandas as pd
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter, SensorEvaluationContext
from .jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os
from resources.sql_io_manager import MySQLDirectConnection
from utils.discord_utils import DiscordUtils
from utils.file_utils import FileUtils


@sensor(job=create_qualifying_prediction_job,
        minimum_interval_seconds=300,
        required_resource_keys={'mysql'})
def create_qualifying_prediction_job_sensor(context):
    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["EVENT_YEAR"]}')

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_event_df["EVENT_YEAR"]}':
        return SkipReason(f"Sensor has already run for {next_event_df['ROUND_NUMBER']} - {next_event_df['EVENT_YEAR']}")

    if next_event_df['EVENT_TYPE_CD'] == 1:
        session_num = 3
    else:
        session_num = 1

    query = FileUtils.file_to_query('quali_prediction_job_sensor')

    query = query.replace('{event_year}', str(next_event_df['EVENT_YEAR']))
    query = query.replace('{round_number}', str(next_event_df['ROUND_NUMBER']))
    query = query.replace('{session_number}', str(session_num))

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn).iloc[0]

    row_count = int(df['RowCount'])

    if row_count != 0:
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_event_df["EVENT_YEAR"]}')
        return RunRequest(
            run_config={'ops': {'session_info': {"config": {'round_number': next_event_df['ROUND_NUMBER'],
                                                            'year': next_event_df['EVENT_YEAR']
                                                            }}}}
        )
    else:
        return SkipReason(f"Practice {session_num} data has not been loaded for {next_event_df['ROUND_NUMBER']} - {next_event_df['EVENT_YEAR']}")

@sensor(job=evaluate_qualifying_prediction_job,
        minimum_interval_seconds=300,
        required_resource_keys={'mysql'})
def evaluate_qualifying_prediction_job_sensor(context):
    calender_query = FileUtils.file_to_query('sql_next_event')
    with context.resources.mysql.get_connection() as conn:
        next_event_df = pd.read_sql(calender_query, conn).iloc[0]

    if context.cursor == '':
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"] - 1} - {next_event_df["EVENT_YEAR"]}')

    if context.cursor == f'{next_event_df["ROUND_NUMBER"]} - {next_event_df["EVENT_YEAR"]}':
        return SkipReason(f"Sensor has already run for {next_event_df['ROUND_NUMBER']} - {next_event_df['EVENT_YEAR']}")

    query = FileUtils.file_to_query('quali_evaluation_job_sensor')

    query = query.replace('{event_year}', str(next_event_df['EVENT_YEAR']))
    query = query.replace('{round_number}', str(next_event_df['ROUND_NUMBER']))

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn).iloc[0]

    row_count = int(df['RowCount'])

    if row_count != 0:
        context.update_cursor(f'{next_event_df["ROUND_NUMBER"]} - {next_event_df["EVENT_YEAR"]}')
        return RunRequest(
            run_config={'ops': {'session_info': {"config": {'round_number': next_event_df['ROUND_NUMBER'],
                                                            'year': next_event_df['EVENT_YEAR']
                                                            }}}}
        )
    else:
        return SkipReason(f"Qualifying data has not been loaded for {next_event_df['ROUND_NUMBER']} - {next_event_df['EVENT_YEAR']}")
