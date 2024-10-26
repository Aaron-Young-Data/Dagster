import pandas as pd
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter
from .jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')
tableau_data_loc = os.getenv('TABLEAU_DATA_LOC')


@sensor(job=session_data_load_job, minimum_interval_seconds=30)
def session_data_load_job_sensor(context):
    # load calender csv into dataframe updated weekly by update_calender_job
    calendar = pd.read_csv(f"{data_loc}calender.csv")
    time_zone = pytz.timezone("GMT")
    naive = datetime.now()
    local_dt = time_zone.localize(naive, is_dst=False)
    utc_dt = local_dt.astimezone(pytz.utc)
    utc_now = datetime.utcnow()
    # this find the closes race in the calendar
    closest_race = calendar[pd.to_datetime(calendar['EventDate']).dt.date > utc_dt.date()].iloc[0]

    if closest_race['EventFormat'] == 'conventional':
        session_list = ['Session1', 'Session2', 'Session3', 'Session4']
    else:
        session_list = ['Session1', 'Session4']

    session_df = pd.DataFrame()
    for session in session_list:
        session_df = pd.concat([session_df,
                                pd.DataFrame({'session_num': [session],
                                              'session_time': [closest_race[f'{session}DateUtc']],
                                              'session_name': [closest_race[session]]})],
                               ignore_index=True)

    session_df['session_time'] = pd.to_datetime(session_df['session_time'])

    try:
        next_session = session_df[session_df['session_time'].dt.tz_localize('UTC') > utc_dt - timedelta(hours=3)].iloc[0]
    except IndexError:
        return SkipReason(f'All sessions have been loaded for {closest_race["EventName"]}')

    if next_session['session_time'].date() != utc_dt.date():
        return SkipReason(f'{closest_race["EventName"]} - {next_session["session_name"]} is not today! Next session dt:'
                          f' {next_session["session_time"].date()}')

    if context.cursor == next_session['session_name']:
        return SkipReason(f"{closest_race['EventName']} - {next_session['session_name']} has already been loaded!")

    session_time = next_session['session_time']

    session_time_modified = (session_time + timedelta(hours=1.5))

    if session_time_modified < utc_now:
        try:
            session_data = fastf1.get_session(year=utc_now.year,
                                              gp=int(closest_race['RoundNumber']),
                                              identifier=next_session['session_name']).load()
        except KeyError:
            return SkipReason("Session data is not available")
        if len(session_data) == 0:
            return SkipReason("Session data is not available")

        context.update_cursor(next_session['session_name'])
        return RunRequest(
            run_config={'ops': {'get_session_data': {"config": {'session': next_session['session_name'],
                                                                'event_name': closest_race['EventName'],
                                                                'year': utc_now.year
                                                                }}}}
        )
    else:
        return SkipReason(f"It is not 30 mins after the {next_session['session_name']}, next session is on "
                          f"{session_time.date} at {session_time.time} what is "
                          f"{(session_time - utc_now).total_seconds()} seconds away!")


@sensor(job=full_session_data_load_job, minimum_interval_seconds=30)
def full_session_data_load_job_sensor(context):
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
        row_count = int(df['RowCount'].iloc[0])
        if row_count == 0:
            return RunRequest(
                job_name='session_data_load_job',
                run_config={'ops': {'get_session_data': {"config": {'year_list': year_list
                                                                    }}}})

        else:
            return SkipReason(f'Current row count: {int(row_count)}')
    else:
        return SkipReason('Job is already running!')
