import os
import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from datetime import datetime
import requests

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def get_track_data_csv(context: AssetExecutionContext):
    track_data = pd.read_csv(f'{data_loc}track_data.csv', encoding='latin-1')
    return Output(value=track_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_data.head().to_markdown()),
                      'Rows': len(track_data)
                  })


@asset()
def get_track_data_api(context: AssetExecutionContext):
    first_year = 2018
    last_year = datetime.today().year

    year_list = [i for i in range(first_year, last_year + 1, 1)]

    context.log.info(f'year_list: {year_list}')

    url = 'https://api.jolpi.ca/ergast/f1/{year}/circuits/'

    track_df = pd.DataFrame()

    for year in year_list:
        response = requests.get(url.replace('{year}', str(year)))
        if response.status_code == 200:
            post = response.json()
        else:
            raise Exception(f'Error: {response.status_code}')
        if response is not None:
            main_data = post['MRData']['CircuitTable']['Circuits']
            df = pd.DataFrame(main_data)
            loc_df = pd.json_normalize(df['Location'])
            final_df = pd.concat([df, loc_df], axis=1).drop(columns='Location')
            track_df = pd.concat([track_df, final_df], ignore_index=True)

    track_df.drop_duplicates(subset='circuitId', inplace=True, ignore_index=True)

    track_df.rename(columns={'circuitId': 'TRACK_ID',
                             'url': 'URL',
                             'circuitName': 'TRACK_NAME',
                             'lat': 'LATITUDE',
                             'long': 'LONGITUDE',
                             'locality': 'LOCATION',
                             'country': 'COUNTRY'
                             },
                    inplace=True)

    return Output(value=track_df,
                  metadata={
                      'Markdown': MetadataValue.md(track_df.head().to_markdown()),
                      'Rows': len(track_df)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'DIM_TRACK', 'cleanup'])
def track_data_to_sql(context: AssetExecutionContext,
                      get_track_data_csv: pd.DataFrame,
                      get_track_data_api: pd.DataFrame):

    df = pd.merge(left=get_track_data_api,
                  right=get_track_data_csv,
                  how='left',
                  on='TRACK_ID')

    df = df[['TRACK_CD',
             'TRACK_ID',
             'TRACK_NAME',
             'LOCATION',
             'COUNTRY',
             'LATITUDE',
             'LONGITUDE',
             'TRACTION',
             'TYRE_STRESS',
             'ASPHALT_GRIP',
             'BRAKING',
             'ASPHALT_ABRASION',
             'LATERAL_FORCE',
             'TRACK_EVOLUTION',
             'DOWNFORCE']]

    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def get_track_event_data_api(context):
    first_year = 2018
    last_year = datetime.today().year

    year_list = [i for i in range(first_year, last_year + 1, 1)]

    context.log.info(f'year_list: {year_list}')

    url = 'https://api.jolpi.ca/ergast/f1/{year}/races/'

    event_df = pd.DataFrame()

    for year in year_list:
        response = requests.get(url.replace('{year}', str(year)))
        if response.status_code == 200:
            post = response.json()
        else:
            raise Exception(f'Error: {response.status_code}')
        if response is not None:
            main_data = post['MRData']['RaceTable']['Races']
            df = pd.DataFrame(main_data)
            loc_df = pd.json_normalize(df['Circuit'])
            final_df = pd.concat([df, loc_df], axis=1).drop(columns='Circuit')[['season', 'round', 'circuitId']]
            event_df = pd.concat([event_df, final_df], ignore_index=True)

    event_df.loc[:, 'EVENT_CD'] = event_df['season'] + event_df['round']

    event_df.drop(columns=['season', 'round'], inplace=True)

    event_df.rename(columns={'circuitId': 'TRACK_ID'}, inplace=True)

    return Output(value=event_df,
                  metadata={
                      'Markdown': MetadataValue.md(event_df.head().to_markdown()),
                      'Rows': len(event_df)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'DIM_TRACK_EVENT', 'cleanup'])
def track_event_data_to_sql(context, get_track_event_data_api: pd.DataFrame):
    df = get_track_event_data_api
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )
