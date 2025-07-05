import os
import pandas as pd
from dagster import asset, Output, MetadataValue
import datetime

data_loc = os.getenv('DATA_STORE_LOC')


@asset()
def get_weather_type_csv(context):
    weather_type_data = pd.read_csv(f'{data_loc}dim_weather_type.csv', encoding='latin-1')
    return Output(value=weather_type_data,
                  metadata={
                      'Markdown': MetadataValue.md(weather_type_data.head().to_markdown()),
                      'Rows': len(weather_type_data)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=['WEATHER', 'DIM_WEATHER_TYPE', 'cleanup'])
def weather_type_to_sql(context,
                        get_weather_type_csv: pd.DataFrame):
    df = get_weather_type_csv
    df['LOAD_TS'] = datetime.datetime.now()
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )
