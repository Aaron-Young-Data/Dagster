import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.file_utils import FileUtils


@asset(required_resource_keys={"mysql"},
       config_schema={'year_list': list})
def get_events_sql(context: AssetExecutionContext):
    year_list = context.op_config['year_list']
    context.log.info(str(year_list))

    query = FileUtils.file_to_query('sql_event_data')
    if len(year_list) == 1:
        query_modified = query.replace('{years}', str(tuple(year_list)).replace(',', ''))
    else:
        query_modified = query.replace('{years}', str(tuple(year_list)))

    context.log.info(f'Running Query: {query_modified}')

    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query_modified, conn)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )

