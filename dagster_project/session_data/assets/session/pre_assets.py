import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.file_utils import FileUtils


@asset(required_resource_keys={"mysql"})
def get_drivers_sql(context: AssetExecutionContext):
    query = FileUtils.file_to_query('sql_driver_data')

    context.log.info(f'Running Query: {query}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset(required_resource_keys={"mysql"})
def get_teams_sql(context: AssetExecutionContext):
    query = FileUtils.file_to_query('sql_teams_data')

    context.log.info(f'Running Query: {query}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )
