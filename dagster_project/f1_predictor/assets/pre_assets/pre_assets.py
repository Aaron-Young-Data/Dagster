import datetime
import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.discord_utils import DiscordUtils
from sklearn.linear_model import LinearRegression
from utils.file_utils import FileUtils
import matplotlib.pyplot as plt

@asset(config_schema={'round_number': int, 'year': int})
def session_info(context: AssetExecutionContext):

    dicts = {
        'round_number': context.op_config['round_number'],
        'year': context.op_config['year']
    }

    return Output(
        value=dicts,
        metadata={
            'round_number': dicts['round_number'],
            'year': dicts['year']
        }
    )

@asset(required_resource_keys={"mysql"})
def sql_driver_data(context: AssetExecutionContext):
    query = FileUtils.file_to_query('sql_driver_data')

    context.log.info(f'Running Query: {query}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )