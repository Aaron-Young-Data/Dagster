import os
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster_project.fast_f1_functions.collect_data import GetData, CleanData
from datetime import date, datetime

data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'year_list': list})
def get_data_analysis_session_data(context):
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
            event_data = pd.DataFrame()
            for session in all_sessions:
                session_data = data.session_data(year=year, location=race, session=session)
                all_laps = data.all_laps(session_data=session_data)
                if len(all_laps) == 0:
                    break

                needed_data = all_laps[['Driver',
                                        'DriverNumber',
                                        'Team',
                                        'LapTime',
                                        'LapNumber',
                                        'Position',
                                        'Stint',
                                        'TyreLife',
                                        'FreshTyre',
                                        'Sector1Time',
                                        'Sector2Time',
                                        'Sector3Time',
                                        'SpeedI1',
                                        'SpeedI2',
                                        'SpeedFL',
                                        'SpeedST',
                                        'IsPersonalBest',
                                        'Compound',
                                        'Deleted',
                                        'DeletedReason',
                                        'AirTemp',
                                        'Rainfall',
                                        'TrackTemp',
                                        'WindDirection',
                                        'WindSpeed',
                                        'TrackStatus',
                                        'IsAccurate']]

                session_df = clean.time_cols_to_seconds(column_names=['LapTime',
                                                                      'Sector1Time',
                                                                      'Sector2Time',
                                                                      'Sector3Time'],
                                                        dataframe=needed_data)

                session_df['session'] = session

                if event_data.empty:
                    event_data = session_df
                else:
                    event_data = pd.concat([event_data, session_df], ignore_index=True)
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


@asset(io_manager_key='sql_io_manager', key_prefix=['tableau_data', 'all_session_data', 'cleanup'])
def data_analysis_session_data_to_sql(context, get_data_analysis_session_data: pd.DataFrame):
    df = get_data_analysis_session_data
    df['load_timestamp'] = datetime.today()
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )


@asset(config_schema={'event_type': str, 'event_name': str, 'year': int})
def get_data_analysis_weekend_session_data(context):
    event_type = context.op_config['event_type']
    event_name = context.op_config['event_name']
    year = context.op_config['year']

    calendar = pd.read_csv(f"{data_loc}calender.csv")

    context.log.info(f'Currently getting event: {event_name} - {year}')
    all_sessions = data.session_list(calendar[calendar['EventName'] == event_name][['Session1',
                                                                                    'Session2',
                                                                                    'Session3',
                                                                                    'Session4',
                                                                                    'Session5']])
    event_data = pd.DataFrame()
    for session in all_sessions:
        session_data = data.session_data(year=year, location=event_name, session=session)
        all_laps = data.all_laps(session_data=session_data)
        if len(all_laps) == 0:
            break

        needed_data = all_laps[['Driver',
                                'DriverNumber',
                                'Team',
                                'LapTime',
                                'LapNumber',
                                'Position',
                                'Stint',
                                'TyreLife',
                                'FreshTyre',
                                'Sector1Time',
                                'Sector2Time',
                                'Sector3Time',
                                'SpeedI1',
                                'SpeedI2',
                                'SpeedFL',
                                'SpeedST',
                                'IsPersonalBest',
                                'Compound',
                                'Deleted',
                                'DeletedReason',
                                'AirTemp',
                                'Rainfall',
                                'TrackTemp',
                                'WindDirection',
                                'WindSpeed',
                                'TrackStatus',
                                'IsAccurate']]

        session_df = clean.time_cols_to_seconds(column_names=['LapTime',
                                                              'Sector1Time',
                                                              'Sector2Time',
                                                              'Sector3Time'],
                                                dataframe=needed_data)

        session_df['session'] = session

        if event_data.empty:
            event_data = session_df
        else:
            event_data = pd.concat([event_data, session_df], ignore_index=True)
    event_data['event_name'] = event_name
    event_data['year'] = year
    event_data['event_type'] = event_type
    return Output(
        value=event_data,
        metadata={
            'Markdown': MetadataValue.md(event_data.head().to_markdown()),
            'Rows': len(event_data)
        }

    )


@asset(io_manager_key='sql_io_manager', key_prefix=['tableau_data', 'all_session_data', 'append'])
def data_analysis_weekend_session_data_to_sql(context, get_data_analysis_weekend_session_data: pd.DataFrame):
    df = get_data_analysis_weekend_session_data
    df['load_timestamp'] = datetime.today()
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )
