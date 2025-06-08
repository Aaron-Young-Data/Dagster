import pandas as pd
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext


@asset(required_resource_keys={"fastf1"},
       config_schema={'practice_num':  int,
                      'round_number': int,
                      'year': int})
def get_practice_data_api(context: AssetExecutionContext):
    practice_num = context.op_config['practice_num']
    round_number = context.op_config['round_number']
    year = context.op_config['year']

    context.log.info(f"Getting Free Practice {practice_num} for round {round_number} - {year}")

    api_data = context.resources.fastf1.get_practice_results(year=year,
                                                             round_number=round_number,
                                                             practice_num=practice_num,
                                                             drivers=False).copy()

    api_data.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))

    return Output(value=api_data,
                  metadata={
                      'Markdown': MetadataValue.md(api_data.head().to_markdown()),
                      'Rows': len(api_data)}
                  )


@asset()
def clean_practice_data(context: AssetExecutionContext,
                        get_practice_data_api: pd.DataFrame,
                        get_drivers_sql: pd.DataFrame,
                        get_teams_sql: pd.DataFrame):
    df = get_practice_data_api
    driver_df = get_drivers_sql
    team_df = get_teams_sql
    # Merging Team and Driver dfs with data df
    context.log.info('Merging practice data with driver and team data')
    df = pd.merge(df, driver_df, how='left', left_on='Driver', right_on='DRIVER_CODE')
    df = pd.merge(df, team_df, how='left', left_on='Team', right_on='NAME')

    # Removing un needed columns
    context.log.info('Removing columns from merge')
    df.drop(columns=['Driver', 'Team', 'DRIVER_CODE', 'NAME', 'DRIVER_NUMBER', 'QUALI_CD'], inplace=True)

    # Rename columns to match the table column names
    context.log.info('Renaming columns')
    df.rename(columns={'LapTime': 'LAPTIME',
                       'Sector1Time': 'SECTOR1_TIME',
                       'Sector2Time': 'SECTOR2_TIME',
                       'Sector3Time': 'SECTOR3_TIME',
                       'CONSTRUCTOR_ID': 'TEAM_ID'},
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


@asset(io_manager_key='sql_io_manager',
       key_prefix=['SESSION', 'PRACTICE_RESULTS', 'append'])
def practice_data_to_sql(context: AssetExecutionContext,
                         clean_practice_data: pd.DataFrame):
    df = clean_practice_data
    df['LOAD_TS'] = datetime.datetime.now()
    context.log.info(f"Loading {len(df)} rows of data into SESSION.PRACTICE_RESULTS")
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
