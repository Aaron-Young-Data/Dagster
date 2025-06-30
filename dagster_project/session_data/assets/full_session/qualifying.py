import pandas as pd
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext


@asset(required_resource_keys={"fastf1"})
def get_full_quali_data_api(context: AssetExecutionContext,
                            get_events_sql: pd.DataFrame):
    df = pd.DataFrame()
    # Go through all the rows in the event df that was passed
    for index, row in get_events_sql.iterrows():
        context.log.info(f"Getting Qualifying for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

        api_data = context.resources.fastf1.get_qualifying_results(year=row['EVENT_YEAR'],
                                                                   round_number=row['ROUND_NUMBER']).copy()

        api_data.loc[:, 'SESSION_CD'] = 4

        if (row['EVENT_TYPE_CD'] == 2) and (row['EVENT_YEAR'] >= 2023):
            context.log.info(f"Getting Sprit Qualifying for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

            driver_df = api_data[['Abbreviation', 'DriverNumber', 'DriverId', 'TeamId']]

            api_sprint_data = context.resources.fastf1.get_qualifying_results(year=row['EVENT_YEAR'],
                                                                              round_number=row['ROUND_NUMBER'],
                                                                              sprint=True).drop(columns=['DriverId',
                                                                                                         'TeamId']).copy()

            api_sprint_data = pd.merge(api_sprint_data, driver_df, on=['Abbreviation', 'DriverNumber'])

            api_sprint_data.loc[:, 'SESSION_CD'] = 5

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
def clean_full_quali_data(context: AssetExecutionContext,
                          get_full_quali_data_api: pd.DataFrame):
    df = get_full_quali_data_api

    # Drop un needed columns
    context.log.info('Deleting columns: ("Abbreviation", "DriverNumber").')
    df.drop(columns=['Abbreviation', 'DriverNumber'], inplace=True)

    # Rename columns to match the table column names
    context.log.info('Renaming columns.')
    df.rename(columns={'DriverId': 'DRIVER_ID',
                       'TeamId': 'TEAM_ID',
                       'Position': 'Q_POSITION',
                       'Q1': 'Q1_LAPTIME',
                       'Q2': 'Q2_LAPTIME',
                       'Q3': 'Q3_LAPTIME'},
              inplace=True)
    
    # Set all the time columns to be seconds
    context.log.info('Setting LapTime columns to seconds.')
    df["Q1_LAPTIME"] = df["Q1_LAPTIME"].dt.total_seconds()
    df["Q2_LAPTIME"] = df["Q2_LAPTIME"].dt.total_seconds()
    df["Q3_LAPTIME"] = df["Q3_LAPTIME"].dt.total_seconds()

    # Create a column with the drivers final qualifying time i.e. Q1 if they didn't get through, Then Q2 etc
    context.log.info('Creating "Q_TIME" column.')
    df['Q_TIME'] = df["Q3_LAPTIME"].fillna(df["Q2_LAPTIME"]).fillna(df["Q1_LAPTIME"])

    # Fill the blanks for all the time columns if they did not set a time.
    cols = ["Q_TIME", "Q3_LAPTIME", "Q2_LAPTIME", "Q1_LAPTIME"]

    context.log.info(f'Filling NULLs in {tuple(cols)} with 0')
    df[cols] = df[cols].fillna(0)

    context.log.info(f'Filling blanks in {tuple(cols)} with 0')
    df[cols] = df[cols].replace('', 0)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset(io_manager_key='sql_io_manager', key_prefix=['SESSION', 'QUALIFYING_RESULTS', 'cleanup'])
def full_quali_data_to_sql(context: AssetExecutionContext,
                           clean_full_quali_data: pd.DataFrame):
    df = clean_full_quali_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.QUALIFYING_RESULTS current records will be deleted.")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
