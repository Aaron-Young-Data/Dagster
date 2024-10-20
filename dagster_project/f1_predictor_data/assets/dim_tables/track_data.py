import os
import pandas as pd
from dagster import asset, Output, MetadataValue
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def get_track_data_csv(context):
    track_data = pd.read_csv(f'{data_loc}track_data.csv', encoding='latin-1')
    return Output(value=track_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_data.head().to_markdown()),
                      'Rows': len(track_data)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'DIM_TRACK', 'cleanup'])
def track_data_to_sql(context, get_track_data_csv: pd.DataFrame):
    df = get_track_data_csv
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )

@asset()
def get_track_event_data_csv(context):
    track_event_data = pd.read_csv(f'{data_loc}event_track_table.csv')
    return Output(value=track_event_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_event_data.head().to_markdown()),
                      'Rows': len(track_event_data)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'DIM_TRACK_EVENT', 'cleanup'])
def track_event_data_to_sql(context, get_track_event_data_csv: pd.DataFrame):
    df = get_track_event_data_csv
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )



