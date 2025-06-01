import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
import datetime


@asset(required_resource_keys={"jolpi_api"})
def get_constructor_data_api(context: AssetExecutionContext):
    first_year = 2018
    last_year = datetime.datetime.today().year

    year_list = [i for i in range(first_year, last_year + 1, 1)]

    context.log.info(f'Year List: {year_list}')

    df = pd.DataFrame()

    for year in year_list:
        df = pd.concat([df, context.resources.jolpi_api.get_constructors(year=year)])

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  })


@asset()
def clean_constructor_data(context: AssetExecutionContext,
                           get_constructor_data_api: pd.DataFrame):
    df = get_constructor_data_api

    df.drop_duplicates(subset='constructorId', inplace=True, ignore_index=True)

    df.rename(columns={'constructorId': 'CONSTRUCTOR_ID',
                       'url': 'CONSTRUCTOR_URL',
                       'name': 'NAME',
                       'nationality': 'NATIONALITY'},
              inplace=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  })


@asset(io_manager_key='sql_io_manager', key_prefix=['REFERENCE', 'DIM_CONSTRUCTOR', 'cleanup'])
def constructor_data_to_sql(context: AssetExecutionContext,
                            clean_constructor_data: pd.DataFrame):
    df = clean_constructor_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into REFERENCE.DIM_CONSTRUCTOR current records will be deleted.")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
