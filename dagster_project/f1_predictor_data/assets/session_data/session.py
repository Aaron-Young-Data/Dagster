import datetime
import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from fast_f1_functions.collect_data import GetData, CleanData
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from datetime import date
from utils.discord_utils import DiscordUtils

data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'year_list': list})
def get_full_session_data(context):
    today = date.today()
    full_data = pd.DataFrame()
    year_list = context.op_config['year_list']
    context.log.info(str(year_list))

    calendar = pd.read_csv(f"{data_loc}calender.csv")

    for year in year_list:
        races_df = calendar[(pd.to_datetime(calendar['EventDate']).dt.date < today) &
                            (pd.to_datetime(calendar['EventDate']).dt.year == year)]

        races = races_df['EventName'].to_list()
        for race in races:
            context.log.info(f'Currently getting event: {race} - {year}')
            all_sessions = data.session_list(races_df[races_df['EventName'] == race][['Session1',
                                                                                      'Session2',
                                                                                      'Session3',
                                                                                      'Session4',
                                                                                      'Session5']])
            sessions_practice = [x for x in all_sessions if 'Practice' in x]
            sessions_practice.append('Qualifying')
            sessions = sessions_practice
            context.log.info(f'Getting sessions: {tuple(sessions)}')
            if 'Practice' in sessions[len(sessions) - 1]:
                sessions = sessions.pop()
            event_data = pd.DataFrame()
            for session in sessions:
                session_data = data.session_data(year=year, location=race, session=session)
                fastest_laps = data.fastest_laps(session_data=session_data)
                if len(fastest_laps) == 0:
                    break
                fastest_laps_ordered = clean.order_laps_delta(laps=fastest_laps, include_pos=False)
                needed_data = fastest_laps_ordered[['Driver',
                                                    'DriverNumber',
                                                    'Team',
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

                session_df['SESSION_CD'] = session

                if event_data.empty:
                    event_data = session_df
                else:
                    event_data = pd.concat([event_data, session_df])

            event_data['EVENT_CD'] = str(year) + str(races_df[races_df['EventName'] == race]['RoundNumber'].iloc[0])
            full_data = pd.concat([full_data, event_data])

    return Output(
        value=full_data,
        metadata={
            'Markdown': MetadataValue.md(full_data.head().to_markdown()),
            'Rows': len(full_data)
        }

    )


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'SESSION_DATA', 'cleanup'])
def full_session_data_to_sql(context, get_full_session_data: pd.DataFrame):
    df = get_full_session_data.rename(columns={
        'Driver': 'DRIVER',
        'DriverNumber': 'DRIVER_NUMBER',
        'Team': 'TEAM',
        'LapTime': 'LAPTIME',
        'Sector1Time': 'SECTOR1_TIME',
        'Sector2Time': 'SECTOR2_TIME',
        'Sector3Time': 'SECTOR3_TIME',
        'Compound': 'COMPOUND',
        'AirTemp': 'AIR_TEMP',
        'Rainfall': 'RAINFALL_FLAG',
        'TrackTemp': 'TRACK_TEMP',
        'WindDirection': 'WIND_DIRECTION',
        'WindSpeed': 'WIND_SPEED'
    })
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )


@asset(config_schema={'session': str, 'event_name': str, 'year': int})
def get_session_data(context):
    session = context.op_config['session']
    event_name = context.op_config['event_name']
    year = context.op_config['year']

    dis = DiscordUtils()
    dis.send_message(message='Getting session data for {} - {}'.format(event_name, session))

    calendar = pd.read_csv(f"{data_loc}calender.csv")
    race_df = calendar[(pd.to_datetime(calendar['EventDate']).dt.year == year) & (calendar['EventName'] == event_name)]

    #if event_type == 'conventional':
    #    session_list = ['Practice 1', 'Practice 2', 'Practice 3', 'Qualifying']
    #else:
    #    session_list = ['Practice 1', 'Qualifying']

    session_data = data.session_data(year=year, location=event_name, session=session)
    fastest_laps = data.fastest_laps(session_data=session_data)
    if len(fastest_laps) == 0:
        return pd.DataFrame()
    fastest_laps_ordered = clean.order_laps_delta(laps=fastest_laps, include_pos=False)
    needed_data = fastest_laps_ordered[['Driver',
                                        'DriverNumber',
                                        'Team',
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

    session_df['SESSION_CD'] = session

    session_df['EVENT_CD'] = str(year) + str(race_df['RoundNumber'].iloc[0])

    return Output(
        value=session_df,
        metadata={
            'Markdown': MetadataValue.md(session_df.head().to_markdown()),
            'Rows': len(session_df)
        }

    )


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'SESSION_DATA'])
def session_data_to_sql(context, get_session_data: pd.DataFrame):
    df = get_session_data.rename(columns={
        'Driver': 'DRIVER',
        'DriverNumber': 'DRIVER_NUMBER',
        'Team': 'TEAM',
        'LapTime': 'LAPTIME',
        'Sector1Time': 'SECTOR1_TIME',
        'Sector2Time': 'SECTOR2_TIME',
        'Sector3Time': 'SECTOR3_TIME',
        'Compound': 'COMPOUND',
        'AirTemp': 'AIR_TEMP',
        'Rainfall': 'RAINFALL_FLAG',
        'TrackTemp': 'TRACK_TEMP',
        'WindDirection': 'WIND_DIRECTION',
        'WindSpeed': 'WIND_SPEED'
    })

    df['LOAD_TS'] = datetime.datetime.now()

    dis = DiscordUtils()
    dis.send_message(message='Loaded {} rows into {}.session_data at {}'.format(len(df),
                                                                                database,
                                                                                df['LOAD_TS'].iloc[0]))

    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )
