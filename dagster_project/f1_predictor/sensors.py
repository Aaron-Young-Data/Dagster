import pandas as pd
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter
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


@sensor(job=create_prediction_job, minimum_interval_seconds=300)
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

    # check the race weekend type
    if closest_race['EventFormat'] == 'conventional':
        # get the session start time
        session_time = datetime.strptime(closest_race['Session3DateUtc'],
                                         '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

        # add 1.5 hours to the session start time 1 hour for the session 30 mins for the data to be available
        session_time_modified = (session_time + timedelta(hours=1.5)).replace(tzinfo=pytz.utc)

        # check if we are after the needed session + 1hr
        if session_time_modified < utc_dt:
            # attempts to collect data and load it
            try:
                session_data = fastf1.get_session(year=naive.year,
                                                  gp=int(closest_race['RoundNumber']),
                                                  identifier='Practice 3').load()
            except KeyError:
                return SkipReason("Session data is not available")
            # update cursor to current data to only allow one run be to done daily
            context.update_cursor(str(date.today()))
            # run the prediction job with event info config
            return RunRequest(
                run_config={'ops': {'session_info': {"config": {'event_type': closest_race['EventFormat'],
                                                                'event_name': closest_race['EventName'],
                                                                'year': naive.year
                                                                }}}}
            )
        else:
            return SkipReason("It is not 30 mins after the session")
    elif closest_race['EventFormat'] != 'conventional':
        # get the session start time
        session_time = datetime.strptime(closest_race['Session1DateUtc'],
                                         '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

        # add 1.5 hours to the session start time 1 hour for the session 30 mins for the data to be available
        session_time_modified = (session_time + timedelta(hours=1.5)).replace(tzinfo=pytz.utc)

        # check if we are after the needed session + 1hr
        if session_time_modified < utc_dt:
            try:
                session_data = fastf1.get_session(year=naive.year,
                                                  gp=int(closest_race['RoundNumber']),
                                                  identifier='Practice 1').load()
            except KeyError:
                return SkipReason("Session data is not available")
            context.update_cursor(str(date.today()))
            return RunRequest(
                run_config={'ops': {'session_info': {"config": {'event_type': closest_race['EventFormat'],
                                                                'event_name': closest_race['EventName'],
                                                                'year': naive.year
                                                                }}}}
            )
        else:
            return SkipReason("It is not 30 mins after the session")


@sensor(job=evaluate_prediction_job, minimum_interval_seconds=300)
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

    # get the next qualifying section date
    quali_dt = pd.to_datetime(closest_race['Session4DateUtc']).date()

    # check if it is qualifying day
    if quali_dt != date.today():
        return SkipReason(f'Qualifying is not today! Next qualifying session dt: {quali_dt}')

    # get the session start time
    session_time = datetime.strptime(closest_race['Session4DateUtc'],
                                     '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

    # add 1.5 hours to the session start time 1 hour for the session 30 mins for the data to be available
    session_time_modified = (session_time + timedelta(hours=1.5)).replace(tzinfo=pytz.utc)

    # check if we are after the needed session + 30 mins
    if session_time_modified < utc_dt:
        # attempts to collect data and load it
        try:
            session_data = fastf1.get_session(year=naive.year,
                                              gp=int(closest_race['RoundNumber']),
                                              identifier='Q').load()
        except KeyError:
            return SkipReason("Session data is not available")
        # update cursor to current data to only allow one run be to done daily
        context.update_cursor(str(date.today()))
        # run the prediction job with event info config
        return RunRequest(
            run_config={'ops': {'quali_session_info': {"config": {'event_name': closest_race['EventName'],
                                                                  'year': naive.year
                                                                  }}}}
        )
    else:
        return SkipReason("It is not 30 mins after the session")

@sensor(job=create_dnn_model_job, minimum_interval_seconds=30)
def create_dnn_model_discord_sensor(context):
    run_records = context.instance.get_run_records(
        RunsFilter(job_name="create_dnn_model_job", statuses=[DagsterRunStatus.STARTED])
    )
    if len(run_records) == 0:
        dis = DiscordUtils()
        message = dis.check_for_message(
            message_content='rerun'
        )
        if message is not None:
            dis = DiscordUtils()
            dis.send_message(message=f'New run of create_dnn_model_job has been launched!')
            return RunRequest()
        else:
            return SkipReason('No message was found not starting run')
    else:
        return SkipReason('Job is already running!')

@sensor(job=create_dnn_model_job, minimum_interval_seconds=300)
def create_dnn_model_sensor(context):

    monitored_jobs = [
        "weekend_session_data_load_job"
    ]

    last_cursor = context.cursor or "0"
    last_timestamp = datetime.fromtimestamp(float(last_cursor))

    for job_name in monitored_jobs:
        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name="weekend_session_data_load_job",
                statuses=[DagsterRunStatus.SUCCESS],
                updated_after=last_timestamp
            ),
            order_by="update_timestamp",
            ascending=False,
        )

        if not run_records:
            return SkipReason(
                f"Job {job_name} has not completed successfully since {last_timestamp}"
            )

    context.update_cursor(str(datetime.utcnow().timestamp()))
    return RunRequest()
