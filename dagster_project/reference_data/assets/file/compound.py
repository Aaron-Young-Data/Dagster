from dagster import asset, Output, MetadataValue
import fastf1
from datetime import date, datetime
import pandas as pd
import os

data_loc = os.getenv('DATA_STORE_LOC')
database = os.getenv('DATABASE')
@asset
def get_compound_data(context):
    df = pd.read_csv(f"{data_loc}dim_compound.csv")
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )



@asset(io_manager_key='sql_io_manager', key_prefix=['REFERENCE', 'DIM_COMPOUND', 'cleanup'])
def compound_to_sql(context, get_compound_data: pd.DataFrame):
    df = get_compound_data
    df['LOAD_TS'] = datetime.now()
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
