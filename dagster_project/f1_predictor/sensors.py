import pandas as pd
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter
from .jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os
from resources.sql_io_manager import MySQLDirectConnection
from utils.discord_utils import DiscordUtils
from utils.file_utils import FileUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')
tableau_data_loc = os.getenv('TABLEAU_DATA_LOC')


@sensor(job=create_prediction_job, minimum_interval_seconds=30)
def create_prediction_job_sensor(context):
    if context.cursor == str(date.today()):
        return SkipReason("Sensor has already run today")

    # load calender csv into dataframe updated weekly by update_calender_job
    calendar = pd.read_csv(f"{data_loc}calender.csv")

    # convert GMT to UTC as calendar data is in UTC
    time_zone = pytz.timezone("GMT")
    naive = datetime.today()
    local_dt = time_zone.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)

    # this find the closes race in the calendar
    closest_race = calendar[pd.to_datetime(calendar['EventDate']).dt.date > utc_dt.date()].iloc[0]

    closest_race['EventDate'] = pd.to_datetime(closest_race['EventDate'])
    event_name = closest_race['EventName']
    event_year = closest_race['EventDate'].year

    if closest_race['EventFormat'] == 'conventional':
        closest_race['Session3DateUtc'] = pd.to_datetime(closest_race['Session3DateUtc'])
        session = closest_race['Session3']
        if closest_race['Session3DateUtc'].date() != utc_dt.date():
            return SkipReason(f'{event_name} - {session} is not today')
    else:
        closest_race['Session1DateUtc'] = pd.to_datetime(closest_race['Session1DateUtc'])
        session = closest_race['Session1']
        if closest_race['Session1DateUtc'].date() != utc_dt.date():
            return SkipReason(f'{event_name} - {session} is not today')

    event_name = closest_race['EventName']
    event_year = closest_race['EventDate'].year

    query = FileUtils.file_to_query('prediction_job_sensor')

    query_modified = query.replace('{event_name}', event_name)
    query_modified = query_modified.replace('{event_year}', str(event_year))
    query_modified = query_modified.replace('{session}', session)

    con = MySQLDirectConnection(port, database, user, password, server)

    df = con.run_query(query=query_modified)

    row_count = int(df['RowCount'].iloc[0])

    if row_count != 0:
        context.update_cursor(str(date.today()))
        return RunRequest(
            run_config={'ops': {'session_info': {"config": {'event_type': closest_race['EventFormat'],
                                                            'event_name': closest_race['EventName'],
                                                            'year': naive.year
                                                            }}}}
        )
    else:
        return SkipReason(f"Data is not available for {event_name} - {session} in MySQL database yet")


@sensor(job=evaluate_prediction_job, minimum_interval_seconds=30)
def evaluate_prediction_job_sensor(context):
    if context.cursor == str(date.today()):
        return SkipReason("Sensor has already run today")

    # load calender csv into dataframe updated weekly by update_calender_job
    calendar = pd.read_csv(f"{data_loc}calender.csv")

    # convert GMT to UTC as calendar data is in UTC
    time_zone = pytz.timezone("GMT")
    naive = datetime.today()
    local_dt = time_zone.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)

    # this find the closes race in the calendar
    closest_race = calendar[pd.to_datetime(calendar['EventDate']).dt.date > utc_dt.date()].iloc[0]

    closest_race['EventDate'] = pd.to_datetime(closest_race['EventDate'])
    closest_race['Session4DateUtc'] = pd.to_datetime(closest_race['Session4DateUtc'])

    event_name = closest_race['EventName']
    event_year = closest_race['EventDate'].year

    if closest_race['Session4DateUtc'].date() != utc_dt.date():
        return SkipReason(f'{event_name} - Qualifying is not today')

    query = FileUtils.file_to_query('prediction_eval_job_sensor')

    query_modified = query.replace('{event_name}', event_name)
    query_modified = query_modified.replace('{event_year}', str(event_year))

    con = MySQLDirectConnection(port, database, user, password, server)

    df = con.run_query(query=query_modified)

    row_count = int(df['RowCount'].iloc[0])

    if row_count != 0:
        context.update_cursor(str(date.today()))
        return RunRequest(
            run_config={'ops': {'quali_session_info': {"config": {'event_name': closest_race['EventName'],
                                                                  'year': naive.year
                                                                  }}}}
        )
    else:
        return SkipReason(f"Qualifying data is not available for {event_name} in MySQL database yet")


@sensor(job=create_dnn_model_job, minimum_interval_seconds=30)
def create_dnn_model_sensor(context):
    if context.cursor == str(date.today()):
        return SkipReason("Sensor has already run today")

    # load calender csv into dataframe updated weekly by update_calender_job
    calendar = pd.read_csv(f"{data_loc}calender.csv")

    # convert GMT to UTC as calendar data is in UTC
    time_zone = pytz.timezone("GMT")
    naive = datetime.today()
    local_dt = time_zone.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)

    # this find the closes race in the calendar
    closest_race = calendar[pd.to_datetime(calendar['EventDate']).dt.date > utc_dt.date()].iloc[0]

    closest_race['EventDate'] = pd.to_datetime(closest_race['EventDate'])
    closest_race['Session4DateUtc'] = pd.to_datetime(closest_race['Session4DateUtc'])

    event_name = closest_race['EventName']
    event_year = closest_race['EventDate'].year

    if closest_race['Session4DateUtc'].date() != utc_dt.date():
        return SkipReason(f'{event_name} - Qualifying is not today')

    query = FileUtils.file_to_query('prediction_eval_job_sensor')

    query_modified = query.replace('{event_name}', event_name)
    query_modified = query_modified.replace('{event_year}', str(event_year))

    con = MySQLDirectConnection(port, database, user, password, server)

    df = con.run_query(query=query_modified)

    row_count = int(df['RowCount'].iloc[0])

    if row_count != 0:
        context.update_cursor(str(date.today()))
        return RunRequest()
    else:
        return SkipReason(f"Qualifying data is not available for {event_name} in MySQL database yet")
