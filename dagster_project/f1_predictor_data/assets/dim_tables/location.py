from dagster import asset, Output, MetadataValue
import fastf1
from datetime import date
import pandas as pd
import os

data_loc = os.getenv('DATA_STORE_LOC')
database = os.getenv('DATABASE')
@asset
def get_location_data(context):
    df = pd.read_csv(f"{data_loc}location_data.csv")
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )



@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'DIM_LOCATION', 'cleanup'])
def location_to_sql(context, get_location_data: pd.DataFrame):
    df = get_location_data
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
