import os
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster_project.resources.sql_io_manager import MySQLDirectConnection
from dagster_project.utils.file_utils import FileUtils

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database_dev')
port = os.getenv('port')
server = os.getenv('server')


@asset()
def get_track_data_csv(context):
    track_data = pd.read_csv(f'{data_loc}track_data.csv')
    return Output(value=track_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_data.head().to_markdown()),
                      'Rows': len(track_data)
                  })


@asset(io_manager_key='sql_io_manager_dev', key_prefix=['ml_project_dev', 'track_data', 'cleanup'])
def track_data_to_sql(context, get_track_data_csv: pd.DataFrame):
    df = get_track_data_csv.drop(columns=['event_name'])
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(io_manager_key='sql_io_manager_dev', key_prefix=['ml_project_dev', 'dim_event', 'cleanup'])
def dim_track_data_to_sql(context, get_track_data_csv: pd.DataFrame):
    df = get_track_data_csv[['event_name', 'event_cd']]
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def get_track_data_sql(context):
    query = FileUtils.file_to_query('sql_track_data')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )
