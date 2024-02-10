import pandas as pd
from dagster import sensor, RunRequest, SkipReason
from dagster_project.jobs import *
from datetime import datetime, date, timedelta
import pytz
import fastf1
import os

data_loc = os.getenv('DATA_STORE_LOC')
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

    # adds one hour to the current time as data is not normally avable till 30 - 60 mins after session
    time_modified = (utc_dt + timedelta(hours=2)).replace(tzinfo=pytz.utc)

    # check the race weekend type
    if closest_race['EventFormat'] == 'conventional':
        session_time = datetime.strptime(closest_race['Session3DateUtc'],
                                         '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
        # check if we are after the needed session + 1hr
        if session_time < time_modified:
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
    elif closest_race['EventFormat'] != 'conventional':
        session_time = datetime.strptime(closest_race['Session1DateUtc'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
        if session_time < time_modified:
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
