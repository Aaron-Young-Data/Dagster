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


@asset(required_resource_keys={"jolpi_api"})
def get_track_data_api(context: AssetExecutionContext):
    first_year = 2018
    last_year = datetime.today().year

    year_list = [i for i in range(first_year, last_year + 1, 1)]

    context.log.info(f'Year List: {year_list}')

    track_df = pd.DataFrame()

    for year in year_list:
        track_df = pd.concat([track_df, context.resources.jolpi_api.get_tracks(year=year)])

    track_df.drop_duplicates(subset='TRACK_ID', inplace=True, ignore_index=True)

    return Output(value=track_df,
                  metadata={
                      'Markdown': MetadataValue.md(track_df.head().to_markdown()),
                      'Rows': len(track_df)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=['REFERENCE', 'DIM_TRACK', 'cleanup'])
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

    df['LOAD_TS'] = datetime.now()

    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset(required_resource_keys={"jolpi_api"})
def get_track_event_data_api(context):
    first_year = 2018
    last_year = datetime.today().year

    year_list = [i for i in range(first_year, last_year + 1, 1)]

    context.log.info(f'year_list: {year_list}')

    event_df = pd.DataFrame()

    for year in year_list:
        event_df = pd.concat([event_df, context.resources.jolpi_api.get_track_event(year=year)], ignore_index=True)

    return Output(value=event_df,
                  metadata={
                      'Markdown': MetadataValue.md(event_df.head().to_markdown()),
                      'Rows': len(event_df)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=['REFERENCE', 'DIM_TRACK_EVENT', 'cleanup'])
def track_event_data_to_sql(context, get_track_event_data_api: pd.DataFrame):
    df = get_track_event_data_api
    df['LOAD_TS'] = datetime.now()
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )
