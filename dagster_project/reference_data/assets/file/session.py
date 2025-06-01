from dagster import asset, Output, MetadataValue
import pandas as pd
import os
import datetime

data_loc = os.getenv('DATA_STORE_LOC')

@asset
def get_dim_session_data(context):
    df = pd.read_csv(f"{data_loc}dim_session.csv")
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )



@asset(io_manager_key='sql_io_manager', key_prefix=['REFERENCE', 'DIM_SESSION', 'cleanup'])
def dim_session_to_sql(context, get_dim_session_data: pd.DataFrame):
    df = get_dim_session_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.PRACTICE_RESULTS current records will be deleted.")
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
