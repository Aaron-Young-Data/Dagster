import pandas as pd
import numpy as np
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.discord_utils import DiscordUtils

@asset(required_resource_keys={"fastf1"},
       config_schema={'round_number': int,
                      'year': int,
                      'sprint': bool})
def get_race_data_api(context: AssetExecutionContext):
    sprint = context.op_config['sprint']
    round_number = context.op_config['round_number']
    year = context.op_config['year']

    if sprint:
        mess = f"Getting Sprint Race Results for round {round_number} - {year}"

        context.log.info(mess)

        dis = DiscordUtils()
        dis.send_message(message=mess)

        api_data = context.resources.fastf1.get_race_results(year=year,
                                                             round_number=round_number,
                                                             sprint=True).copy()

        api_data.loc[:, 'SESSION_CD'] = 6
        api_data.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))
    else:
        mess = f"Getting Race Results for round {round_number} - {year}"

        context.log.info(mess)

        dis = DiscordUtils()
        dis.send_message(message=mess)

        api_data = context.resources.fastf1.get_race_results(year=year,
                                                             round_number=round_number).copy()

        api_data.loc[:, 'SESSION_CD'] = 7
        api_data.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))

    return Output(value=api_data,
                  metadata={
                      'Markdown': MetadataValue.md(api_data.head().to_markdown()),
                      'Rows': len(api_data)}
                  )


@asset()
def clean_race_data(context: AssetExecutionContext,
                    get_race_data_api: pd.DataFrame):
    df = get_race_data_api

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


@asset(io_manager_key='sql_io_manager',
       key_prefix=['SESSION', 'RACE_RESULTS', 'append'])
def race_data_to_sql(context: AssetExecutionContext,
                     clean_race_data: pd.DataFrame):
    df = clean_race_data
    df['LOAD_TS'] = datetime.datetime.now()

    mess = f"Loading {len(df)} rows of data into SESSION.RACE_RESULTS"

    context.log.info(mess)

    dis = DiscordUtils()
    dis.send_message(message=mess)
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )


@asset(required_resource_keys={"fastf1"},
       config_schema={'round_number': int,
                      'year': int,
                      'sprint': bool})
def get_race_lap_data_api(context: AssetExecutionContext):
    sprint = context.op_config['sprint']
    round_number = context.op_config['round_number']
    year = context.op_config['year']

    if sprint:
        mess = f"Getting Sprint Race Laps for round {round_number} - {year}"
    else:
        mess = f"Getting Race Laps for round {round_number} - {year}"

    context.log.info(mess)

    dis = DiscordUtils()
    dis.send_message(message=mess)

    df = context.resources.fastf1.get_race_results(year=year,
                                                   round_number=round_number,
                                                   sprint=sprint,
                                                   laps=True).copy()
    if sprint:
        df.loc[:, 'SESSION_CD'] = 6
    else:
        df.loc[:, 'SESSION_CD'] = 7

    df.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )


@asset()
def clean_race_lap_data(context: AssetExecutionContext,
                        get_race_lap_data_api: pd.DataFrame,
                        get_drivers_sql: pd.DataFrame,
                        get_teams_sql: pd.DataFrame):
    df = get_race_lap_data_api
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
                     'NAME',
                     'QUALI_CD',
                     'DRIVER_NUMBER',
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
       key_prefix=['SESSION', 'RACE_LAPS', 'append'])
def race_lap_data_to_sql(context: AssetExecutionContext,
                         clean_race_lap_data: pd.DataFrame):
    df = clean_race_lap_data
    df['LOAD_TS'] = datetime.datetime.now()
    mess = f"Loading {len(df)} rows of data into SESSION.RACE_LAPS"

    context.log.info(mess)

    dis = DiscordUtils()
    dis.send_message(message=mess)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
