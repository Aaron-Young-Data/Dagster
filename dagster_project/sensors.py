import pandas as pd
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter
from dagster_project.jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os
from dagster_project.resources.sql_io_manager import MySQLDirectConnection

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


@sensor(job=weekend_session_data_load_job, minimum_interval_seconds=300)
def weekend_session_data_load_job_sensor(context):
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
            run_config={'ops': {'get_session_data_weekend': {"config": {'event_type': closest_race['EventFormat'],
                                                                        'event_name': closest_race['EventName'],
                                                                        'year': naive.year
                                                                        }}}}
        )
    else:
        return SkipReason("It is not 30 mins after the session")


@sensor(job=session_data_load_job, minimum_interval_seconds=300)
def session_data_load_job_sensor(context):
    today = date.today()
    year = today.year
    year_list = list(range(2018, year + 1))
    run_records = context.instance.get_run_records(
        RunsFilter(job_name="session_data_load_job", statuses=[DagsterRunStatus.STARTED])
    )
    if len(run_records) == 0:
        query = FileUtils.file_to_query('session_data_sensor')
        con = MySQLDirectConnection(port, database, user, password, server)
        df = con.run_query(query=query)
        row_count = int(df['RowCount'])
        if row_count == 0:
            return RunRequest(
                job_name='session_data_load_job',
                run_config={'ops': {'get_session_data': {"config": {'year_list': year_list
                                                                    }}}})

        else:
            return SkipReason(f'Current row count: {int(row_count)}')
    else:
        return SkipReason('Job is already running!')


@sensor(job=load_data_analysis_data_job, minimum_interval_seconds=300)
def analytics_session_data_load_job_sensor(context):
    today = date.today()
    year = today.year
    year_list = list(range(2018, year + 1))
    run_records = context.instance.get_run_records(
        RunsFilter(job_name="load_data_analysis_data_job", statuses=[DagsterRunStatus.STARTED])
    )
    if len(run_records) == 0:
        query = FileUtils.file_to_query('analytics_session_data_sensor')
        con = MySQLDirectConnection(port, database, user, password, server)
        df = con.run_query(query=query)
        row_count = int(df['RowCount'])
        if row_count == 0:
            return RunRequest(
                job_name='load_data_analysis_data_job',
                run_config={'ops': {'get_data_analysis_session_data': {"config": {'year_list': year_list
                                                                                  }}}})

        else:
            return SkipReason(f'Current row count: {int(row_count)}')
    else:
        return SkipReason('Job is already running!')


@sensor(job=download_all_session_data_job_analytics, minimum_interval_seconds=300)
def analytics_session_data_download_job_sensor(context):
    run_records = context.instance.get_run_records(
        RunsFilter(job_name="load_data_analysis_data_job", statuses=[DagsterRunStatus.STARTED])
    )
    if len(run_records) == 0:
        try:
            download_file = pd.read_csv(tableau_data_loc + 'Lap_Data.csv')
        except FileNotFoundError:
            return RunRequest(
                job_name='download_all_session_data_job_analytics')
        file_length = len(download_file)
        query = FileUtils.file_to_query('analytics_session_data_download_sensor')
        con = MySQLDirectConnection(port, database, user, password, server)
        df = con.run_query(query=query)
        row_count = int(df['RowCount'])
        if row_count != file_length:
            return RunRequest(
                job_name='download_all_session_data_job')

        else:
            return SkipReason(f'Current row count: {int(row_count)}')
    else:
        return SkipReason('Job is already running!')


@sensor(job=load_weekend_session_data_analytics, minimum_interval_seconds=300)
def analytics_weekend_session_data_load_job_sensor(context):
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

    # get the next race section date
    race_dt = pd.to_datetime(closest_race['Session5DateUtc']).date()

    # check if it is qualifying day
    if race_dt != date.today():
        return SkipReason(f'The race is not today! The next rase session dt: {race_dt}')

    # get the session start time
    session_time = datetime.strptime(closest_race['Session5DateUtc'],
                                     '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

    # add 1.5 hours to the session start time 1 hour for the session 30 mins for the data to be available
    session_time_modified = (session_time + timedelta(hours=1.5)).replace(tzinfo=pytz.utc)

    # check if we are after the needed session + 30 mins
    if session_time_modified < utc_dt:
        # attempts to collect data and load it
        try:
            session_data = fastf1.get_session(year=naive.year,
                                              gp=int(closest_race['RoundNumber']),
                                              identifier='R').load()
        except KeyError:
            return SkipReason("Session data is not available")
        # update cursor to current data to only allow one run be to done daily
        context.update_cursor(str(date.today()))
        # run the prediction job with event info config
        return RunRequest(
            run_config={
                'ops': {'get_data_analysis_weekend_session_data': {"config": {'event_type': closest_race['EventFormat'],
                                                                              'event_name': closest_race['EventName'],
                                                                              'year': naive.year
                                                                              }}}}
        )
    else:
        return SkipReason("It is not 30 mins after the session")
