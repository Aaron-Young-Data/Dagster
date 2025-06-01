import numpy as np
import pandas as pd
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext


@asset(required_resource_keys={"fastf1"})
def get_full_race_data_api(context: AssetExecutionContext,
                           get_events_sql: pd.DataFrame):
    df = pd.DataFrame()
    # Go through all the rows in the event df that was passed
    for index, row in get_events_sql.iterrows():
        context.log.info(f"Getting Race Results for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

        api_data = context.resources.fastf1.get_race_results(year=row['EVENT_YEAR'],
                                                             round_number=row['ROUND_NUMBER']).copy()

        api_data.loc[:, 'SESSION_CD'] = 7

        if (row['EVENT_TYPE_CD'] == 2) and (row['EVENT_YEAR'] >= 2023):
            context.log.info(f"Getting Sprit Race Results for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

            api_sprint_data = context.resources.fastf1.get_race_results(year=row['EVENT_YEAR'],
                                                                        round_number=row['ROUND_NUMBER'],
                                                                        sprint=True).copy()

            api_sprint_data.loc[:, 'SESSION_CD'] = 6

            api_data = pd.concat([api_data, api_sprint_data],
                                 ignore_index=True)

        api_data.loc[:, 'EVENT_CD'] = row['EVENT_CD']

        df = pd.concat([df, api_data],
                       ignore_index=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset()
def clean_full_race_data(context: AssetExecutionContext,
                         get_full_race_data_api: pd.DataFrame):
    df = get_full_race_data_api

    # Drop un needed columns
    context.log.info('Renaming columns.')
    df.rename(columns={'DriverId': 'DRIVER_ID',
                       'TeamId': 'TEAM_ID',
                       'ClassifiedPosition': 'CLASSIFIED_POSITION',
                       'Position': 'POSITION',
                       'Time': 'DELTA',
                       'Status': 'STATUS',
                       'Points': 'POINTS'},
              inplace=True)

    # Set all the time columns to be seconds
    context.log.info('Setting LapTime columns to seconds.')
    df["DELTA"] = df["DELTA"].dt.total_seconds()

    context.log.info('Updating DELTA and TOTAL_TIME column')
    leaders_time = df.groupby('EVENT_CD')['DELTA'].max().reset_index().rename(columns={'DELTA': 'TOTAL_TIME'})
    df = pd.merge(df, leaders_time, on='EVENT_CD', how='left')
    df.loc[df['POSITION'] == 1, 'DELTA'] = 0
    df.loc[:, 'TOTAL_TIME'] = df['TOTAL_TIME'] + df['DELTA']
    df.loc[df['STATUS'] != 'Finished', 'TOTAL_TIME'] = np.nan
    df.loc[df['STATUS'] != 'Finished', 'DELTA'] = np.nan

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset(io_manager_key='sql_io_manager', key_prefix=['SESSION', 'RACE_RESULTS', 'cleanup'])
def full_race_data_to_sql(context: AssetExecutionContext,
                          clean_full_race_data: pd.DataFrame):
    df = clean_full_race_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.RACE_RESULTS current records will be deleted.")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
