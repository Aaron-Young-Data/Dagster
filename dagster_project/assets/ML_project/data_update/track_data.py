import os
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster_project.resources.sql_io_manager import MySQLDirectConnection
from dagster_project.utils.file_utils import FileUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def get_track_data_csv(context):
    track_data = pd.read_csv(f'{data_loc}track_data.csv')
    return Output(value=track_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_data.head().to_markdown()),
                      'Rows': len(track_data)
                  })


@asset(io_manager_key='sql_io_manager_dev', key_prefix=[database, 'track_data', 'cleanup'])
def track_data_to_sql(context, get_track_data_csv: pd.DataFrame):
    df = get_track_data_csv.drop(columns=['event_name'])
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(io_manager_key='sql_io_manager_dev', key_prefix=[database, 'dim_event', 'cleanup'])
def dim_track_data_to_sql(context, get_track_data_csv: pd.DataFrame):
    df = get_track_data_csv[['event_name', 'event_cd']]
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


