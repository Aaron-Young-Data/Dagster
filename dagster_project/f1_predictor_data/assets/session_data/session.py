import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from fast_f1_functions.collect_data import GetData, CleanData
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from datetime import date

data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'year_list': list})
def get_session_data(context):
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
            event_type = races_df[races_df['EventName'] == race]['EventFormat'].to_list()[0]
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

                try:
                    suffix = "FP" + str(int(session[-1:]))
                except:
                    suffix = 'Q'

                if event_data.empty:
                    event_data = session_df.add_suffix(suffix)
                    event_data = event_data.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                    event_data = event_data.rename(columns={f'Team{suffix}': 'Team'})
                    event_data = event_data.rename(columns={f'Driver{suffix}': 'Driver'})
                else:
                    session_df = session_df.add_suffix(suffix)
                    session_df = session_df.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                    session_df = session_df.rename(columns={f'Team{suffix}': 'Team'})
                    session_df = session_df.rename(columns={f'Driver{suffix}': 'Driver'})
                    event_data = pd.merge(event_data, session_df, on=['DriverNumber', 'Driver', 'Team'], how="outer")

            event_data['event_cd'] = str(races_df[races_df['EventName'] == race]['RoundNumber'].iloc[0]) + str(year)
            full_data = pd.concat([full_data, event_data])
    return Output(
        value=full_data,
        metadata={
            'Markdown': MetadataValue.md(full_data.head().to_markdown()),
            'Rows': len(full_data)
        }

    )


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'raw_session_data', 'cleanup'])
def session_data_to_sql(context, get_session_data: pd.DataFrame):
    df = get_session_data
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )


@asset(config_schema={'event_type': str, 'event_name': str, 'year': int})
def get_session_data_weekend(context):
    event_type = context.op_config['event_type']
    event_name = context.op_config['event_name']
    year = context.op_config['year']

    calendar = pd.read_csv(f"{data_loc}calender.csv")
    race_df = calendar[(pd.to_datetime(calendar['EventDate']).dt.year == year & calendar['EventName'] == event_name)]
    if event_type == 'conventional':
        session_list = ['FP1', 'FP2', 'FP3', 'Q']
    else:
        session_list = ['FP1', 'Q']

    event_data = pd.DataFrame()
    for session in session_list:
        session_data = data.session_data(year=year, location=event_name, session=session)
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

    event_data['event_cd'] = str(race_df['RoundNumber'].iloc[0]) + str(year)
    return Output(
        value=event_data,
        metadata={
            'Markdown': MetadataValue.md(event_data.head().to_markdown()),
            'Rows': len(event_data)
        }

    )


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'raw_session_data'])
def session_data_to_sql_append(context, get_session_data_weekend: pd.DataFrame):
    df = get_session_data_weekend
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )
