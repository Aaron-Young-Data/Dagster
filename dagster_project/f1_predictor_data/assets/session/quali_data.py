import datetime
import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut, AssetExecutionContext
from resources.sql_io_manager import MySQLDirectConnection
from utils.fastf1_utils import FastF1Utils
from utils.discord_utils import DiscordUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')

@asset(config_schema={'session': str,
                      'event_name': str,
                      'year': int})
def get_quali_data(context: AssetExecutionContext):
    session = context.op_config['session']
    event_name = context.op_config['event_name']
    year = context.op_config['year']

    calendar = pd.read_csv(f"{data_loc}calender.csv")
    race = calendar[(pd.to_datetime(calendar['EventDate']).dt.year == year) & (calendar['EventName'] == event_name)]

    context.log.info('Getting session data for {} - {}'.format(event_name, session))

    f1 = FastF1Utils(year=year,
                     location=event_name,
                     session=session)

    results = f1.get_session_results()

    results['SESSION_CD'] = session
    results['EVENT_CD'] = str(year) + str(race[race['EventName'] == event_name]['RoundNumber'].iloc[0])

    return Output(
        value=results,
        metadata={
            'Markdown': MetadataValue.md(results.head().to_markdown()),
            'Rows': len(results)
        }

    )


@asset(io_manager_key='sql_io_manager',
       key_prefix=['SESSION', 'QUALIFYING_DATA', 'append'])
def get_quali_data_to_sql(context: AssetExecutionContext,
                          get_quali_data: pd.DataFrame):
    df = get_quali_data.rename(columns={'DriverNumber': 'DRIVER_NUMBER',
                                        'Abbreviation': 'DRIVER',
                                        'TeamName': 'TEAM',
                                        'Q1': 'Q1_LAPTIME',
                                        'Q2': 'Q2_LAPTIME',
                                        'Q3': 'Q3_LAPTIME',
                                        'Q_time': 'Q_TIME',
                                        'Position': 'Q_POSITION',
                                        'Compound': 'COMPOUND',
                                        'AirTemp': 'AIR_TEMP',
                                        'TrackTemp': 'TRACK_TEMP',
                                        'Rainfall': 'RAINFALL_FLAG',
                                        'WindDirection': 'WIND_DIRECTION',
                                        'WindSpeed': 'WIND_SPEED'})

    df.loc[:, 'LOAD_TS'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    context.log.info('Loading {} rows into SESSION.QUALIFYING_DATA'.format(df.shape[0]))

    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df)
        }

    )


@asset(config_schema={'year_list': list})
def get_full_quali_data(context: AssetExecutionContext):
    today = datetime.date.today()
    full_data = pd.DataFrame()
    year_list = context.op_config['year_list']
    context.log.info(str(year_list))

    calendar = pd.read_csv(f"{data_loc}calender.csv")

    for year in year_list:
        races_df = calendar[(pd.to_datetime(calendar['EventDate']).dt.date < today) &
                            (pd.to_datetime(calendar['EventDate']).dt.year == year)]

        races = races_df['EventName'].to_list()