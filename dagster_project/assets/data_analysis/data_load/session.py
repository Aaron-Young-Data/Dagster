import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from dagster_project.fast_f1_functions.collect_data import GetData, CleanData
from dagster_project.resources.sql_io_manager import MySQLDirectConnection
from dagster_project.utils.file_utils import FileUtils
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
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )
