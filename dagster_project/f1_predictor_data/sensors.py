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

