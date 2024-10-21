from dagster import asset, Output, MetadataValue
import pandas as pd
import os

data_loc = os.getenv('DATA_STORE_LOC')
database = os.getenv('DATABASE')
@asset
def get_track_status_data_csv(context):
    df = pd.read_csv(f"{data_loc}track_status.csv")
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )



@asset(io_manager_key='sql_io_manager', key_prefix=['TABLEAU_DATA', 'DIM_TRACK_STATUS', 'cleanup'])
def track_status_data_to_sql(context, get_track_status_data_csv: pd.DataFrame):
    df = get_track_status_data_csv
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
