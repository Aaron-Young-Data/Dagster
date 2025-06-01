import pandas as pd
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext


@asset(required_resource_keys={"fastf1"})
def get_full_practice_data_api(context: AssetExecutionContext,
                               get_events_sql: pd.DataFrame):
    df = pd.DataFrame()
    for index, row in get_events_sql.iterrows():
        context.log.info(f"Getting all practice sessions for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

        api_data = context.resources.fastf1.get_practice_results(year=row['EVENT_YEAR'],
                                                                 round_number=row['ROUND_NUMBER'],
                                                                 drivers=False).copy()

        api_data.loc[:, 'EVENT_CD'] = row['EVENT_CD']

        df = pd.concat([df, api_data],
                       ignore_index=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset()
def clean_full_practice_data(context: AssetExecutionContext,
                             get_full_practice_data_api: pd.DataFrame):
    df = get_full_practice_data_api

    # Rename columns to match the table column names
    context.log.info('Renaming columns')
    df.rename(columns={'DriverId': 'DRIVER_ID',
                       'TeamId': 'TEAM_ID',
                       'LapTime': 'LAPTIME',
                       'Sector1Time': 'SECTOR1_TIME',
                       'Sector2Time': 'SECTOR2_TIME',
                       'Sector3Time': 'SECTOR3_TIME'},
              inplace=True)

    context.log.info('Setting LapTime columns to seconds')
    df["LAPTIME"] = df["LAPTIME"].dt.total_seconds()
    df["SECTOR1_TIME"] = df["SECTOR1_TIME"].dt.total_seconds()
    df["SECTOR2_TIME"] = df["SECTOR2_TIME"].dt.total_seconds()
    df["SECTOR3_TIME"] = df["SECTOR3_TIME"].dt.total_seconds()

    context.log.info('Creating Position column')
    df['POSITION'] = df.groupby(['EVENT_CD', 'SESSION_CD'])["LAPTIME"].rank(method='first')

    context.log.info('Sorting DataFrame')
    df.sort_values(by=['EVENT_CD', 'SESSION_CD', 'POSITION'], inplace=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset(io_manager_key='sql_io_manager', key_prefix=['SESSION', 'PRACTICE_RESULTS', 'cleanup'])
def full_practice_data_to_sql(context: AssetExecutionContext,
                              clean_full_practice_data: pd.DataFrame):
    df = clean_full_practice_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.PRACTICE_RESULTS current records will be deleted.")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
