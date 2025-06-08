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


@asset(required_resource_keys={"fastf1"})
def get_full_race_lap_data_api(context: AssetExecutionContext,
                               get_events_sql: pd.DataFrame):
    df = pd.DataFrame()
    # Go through all the rows in the event df that was passed
    for index, row in get_events_sql.iterrows():
        context.log.info(f"Getting Race Results for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

        api_data = context.resources.fastf1.get_race_results(year=row['EVENT_YEAR'],
                                                             round_number=row['ROUND_NUMBER'],
                                                             laps=True).copy()

        api_data.loc[:, 'SESSION_CD'] = 7

        if (row['EVENT_TYPE_CD'] == 2) and (row['EVENT_YEAR'] >= 2023):
            context.log.info(f"Getting Sprit Race Results for {row['EVENT_YEAR']} - {row['EVENT_NAME']}")

            api_sprint_data = context.resources.fastf1.get_race_results(year=row['EVENT_YEAR'],
                                                                        round_number=row['ROUND_NUMBER'],
                                                                        sprint=True,
                                                                        laps=True).copy()

            api_sprint_data.loc[:, 'SESSION_CD'] = 6

            api_data = pd.concat([api_data, api_sprint_data],
                                 ignore_index=True)

        api_data.loc[:, 'EVENT_CD'] = row['EVENT_CD']

        df = pd.concat([df, api_data],
                       ignore_index=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )


@asset()
def clean_full_race_lap_data(context: AssetExecutionContext,
                             get_full_race_lap_data_api: pd.DataFrame,
                             get_drivers_sql: pd.DataFrame,
                             get_teams_sql: pd.DataFrame):
    df = get_full_race_lap_data_api
    driver_df = get_drivers_sql
    team_df = get_teams_sql
    # Merging Team and Driver dfs with data df
    context.log.info('Merging race lap data with driver and team data')
    df = pd.merge(df, driver_df, how='left', left_on='Driver', right_on='DRIVER_CODE')
    df = pd.merge(df, team_df, how='left', left_on='Team', right_on='NAME')

    context.log.info('Creating Pit In/Out Columns')
    df.loc[~df['PitInTime'].isna(), 'PIT_IN_FLG'] = 1
    df.loc[df['PitInTime'].isna(), 'PIT_IN_FLG'] = 0
    df.loc[~df['PitOutTime'].isna(), 'PIT_OUT_FLG'] = 1
    df.loc[df['PitOutTime'].isna(), 'PIT_OUT_FLG'] = 0

    context.log.info('Setting LapTime columns to seconds.')
    df["LapTime"] = df["LapTime"].dt.total_seconds()
    df["Sector1Time"] = df["Sector1Time"].dt.total_seconds()
    df["Sector2Time"] = df["Sector2Time"].dt.total_seconds()
    df["Sector3Time"] = df["Sector3Time"].dt.total_seconds()

    context.log.info('Setting bool columns to 1 and 0.')
    df["Deleted"] = df["Deleted"].astype(int)
    df["IsAccurate"] = df["IsAccurate"].astype(int)

    context.log.info('Setting blanks in track status to -2')
    df["TrackStatus"] = df["TrackStatus"].replace('', -2)

    context.log.info('Removing unused columns')
    df.drop(columns=['Driver',
                     'Team',
                     'DRIVER_CODE',
                     'QUALI_CD',
                     'DRIVER_NUMBER',
                     'NAME',
                     'PitInTime',
                     'PitOutTime',
                     'Time',
                     'DriverNumber',
                     'Sector1SessionTime',
                     'Sector2SessionTime',
                     'Sector3SessionTime',
                     'IsPersonalBest',
                     'FreshTyre',
                     'LapStartTime',
                     'FastF1Generated'], inplace=True)

    context.log.info('Renaming columns')
    df.rename(columns={'LapNumber': 'LAP_NUMBER',
                       'CONSTRUCTOR_ID': 'TEAM_ID',
                       'Stint': 'STINT_NUMBER',
                       'LapTime': 'LAPTIME',
                       'Sector1Time': 'SECTOR1_TIME',
                       'Sector2Time': 'SECTOR2_TIME',
                       'Sector3Time': 'SECTOR3_TIME',
                       'SpeedI1': 'SPEED_TRAP_1',
                       'SpeedI2': 'SPEED_TRAP_2',
                       'SpeedFL': 'SPEED_TRAP_FLAG',
                       'SpeedST': 'SPEED_TRAP_STRAIGHT',
                       'Compound': 'COMPOUND',
                       'TyreLife': 'TYRE_LIFE',
                       'LapStartDate': 'LAP_START_DATETIME',
                       'TrackStatus': 'TRACK_STATUS',
                       'Position': 'POSITION',
                       'Deleted': 'LAP_DELETED',
                       'DeletedReason': 'LAP_DELETED_REASON',
                       'IsAccurate': 'FF1_LAP_IS_ACCURATE'},
              inplace=True)

    context.log.info('Removing rows where laptime is null')
    df.dropna(subset=['LAPTIME'],
              inplace=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )


@asset(io_manager_key='sql_io_manager',
       key_prefix=['SESSION', 'RACE_LAPS', 'cleanup'])
def full_race_lap_data_to_sql(context: AssetExecutionContext,
                              clean_full_race_lap_data: pd.DataFrame):
    df = clean_full_race_lap_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.RACE_LAPS")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
