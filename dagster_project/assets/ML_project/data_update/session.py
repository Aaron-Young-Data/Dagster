import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from dagster_project.fast_f1_functions.collect_data import GetData, CleanData
from dagster_project.resources.sql_io_manager import MySQLDirectConnection
from dagster_project.utils.file_utils import FileUtils

data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database_dev')
port = os.getenv('port')
server = os.getenv('server')


@asset(config_schema={'year_list': list})
def get_session_data(context):
    full_data = pd.DataFrame()
    year_list = context.op_config['year_list']
    context.log.info(str(year_list))
    for year in year_list:
        context.log.info(year)
        calendar = data.get_calender(year)
        races = calendar['EventName'].to_list()
        for race in races:
            event_type = calendar[calendar['EventName'] == race]['EventFormat'].to_list()[0]
            all_sessions = data.session_list(calendar[calendar['EventName'] == race][['Session1',
                                                                                      'Session2',
                                                                                      'Session3',
                                                                                      'Session4',
                                                                                      'Session5']])
            sessions_practice = [x for x in all_sessions if 'Practice' in x]
            sessions_practice.append('Qualifying')
            sessions = sessions_practice
            if 'Practice' in sessions[len(sessions) - 1]:
                sessions = sessions.pop()
            event_data = pd.DataFrame()
            for session in sessions:
                session_data = data.session_data(year=year, location=race, session=session)
                fastest_laps = data.fastest_laps(session_data=session_data)
                if len(fastest_laps) == 0:
                    break
                fastest_laps_ordered = clean.order_laps_delta(laps=fastest_laps, include_pos=False)
                needed_data = fastest_laps_ordered[['DriverNumber',
                                                    'LapTime',
                                                    'Sector1Time',
                                                    'Sector2Time',
                                                    'Sector3Time',
                                                    'Compound',
                                                    'AirTemp',
                                                    'Rainfall',
                                                    'TrackTemp',
                                                    'WindDirection',
                                                    'WindSpeed']]
                session_df = clean.time_cols_to_seconds(column_names=['LapTime',
                                                                      'Sector1Time',
                                                                      'Sector2Time',
                                                                      'Sector3Time'],
                                                        dataframe=needed_data)

                try:
                    suffix = "FP" + str(int(session[-1:]))
                except:
                    suffix = 'Q'

                if event_data.empty:
                    event_data = session_df.add_suffix(suffix)
                    event_data = event_data.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                else:
                    session_df = session_df.add_suffix(suffix)
                    session_df = session_df.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                    event_data = pd.merge(event_data, session_df, on='DriverNumber', how="outer")
            event_data['event_name'] = race
            event_data['year'] = year
            event_data['event_type'] = event_type
            full_data = pd.concat([full_data, event_data])
    return Output(
        value=full_data,
        metadata={
            'Markdown': MetadataValue.md(full_data.head().to_markdown()),
            'Rows': len(full_data)
        }

    )


@asset(io_manager_key='sql_io_manager_dev', key_prefix=[database, 'raw_session_data', 'cleanup'])
def session_data_to_sql(context, get_session_data: pd.DataFrame):
    df = get_session_data
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )


