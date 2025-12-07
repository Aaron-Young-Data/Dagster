import pandas as pd
import datetime
from dagster import asset, Output, MetadataValue, AssetExecutionContext
from utils.discord_utils import DiscordUtils


@asset(required_resource_keys={"fastf1"},
       config_schema={'round_number': int,
                      'year': int,
                      'sprint': bool})
def get_quali_data_api(context: AssetExecutionContext):
    sprint = context.op_config['sprint']
    round_number = context.op_config['round_number']
    year = context.op_config['year']

    if sprint:
        mess = f"Getting Sprint Qualifying for round {round_number} - {year}"

        context.log.info(mess)

        dis = DiscordUtils()
        dis.send_message(message=mess)

        api_data = context.resources.fastf1.get_qualifying_results(year=year,
                                                                   round_number=round_number,
                                                                   sprint=True).copy()

        api_data.loc[:, 'SESSION_CD'] = 5
        api_data.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))
    else:
        mess = f"Getting Qualifying for round {round_number} - {year}"

        context.log.info(mess)

        dis = DiscordUtils()
        dis.send_message(message=mess)

        api_data = context.resources.fastf1.get_qualifying_results(year=year,
                                                                   round_number=round_number).copy()

        api_data.loc[:, 'SESSION_CD'] = 4
        api_data.loc[:, 'EVENT_CD'] = int(str(year) + str(round_number))

    return Output(value=api_data,
                  metadata={
                      'Markdown': MetadataValue.md(api_data.head().to_markdown()),
                      'Rows': len(api_data)}
                  )


@asset()
def clean_quali_data(context: AssetExecutionContext,
                     get_quali_data_api: pd.DataFrame,
                     get_drivers_sql: pd.DataFrame,
                     get_teams_sql: pd.DataFrame):
    df = get_quali_data_api
    driver_df = get_drivers_sql
    team_df = get_teams_sql

    # Merging Team and Driver dfs with data df
    context.log.info('Merging practice data with driver and team data')
    df = pd.merge(df, driver_df, how='left', left_on='Abbreviation', right_on='DRIVER_CODE')
    df = pd.merge(df, team_df, how='left', left_on='TeamName', right_on='NAME')

    # Removing un needed columns
    context.log.info('Removing columns from merge')
    df.drop(columns=['Abbreviation', 'TeamName', 'DRIVER_CODE', 'NAME', 'DRIVER_NUMBER', 'QUALI_CD'], inplace=True)

    # Rename columns to match the table column names
    context.log.info('Renaming columns.')
    df.rename(columns={'DriverId': 'DRIVER_ID',
                       'CONSTRUCTOR_ID': 'TEAM_ID',
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

    context.log.info(f'Dropping rows where there are Nulls')
    df.dropna(how='any', inplace=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)}
                  )


@asset(io_manager_key='sql_io_manager',
       key_prefix=['SESSION', 'QUALIFYING_RESULTS', 'append'])
def quali_data_to_sql(context: AssetExecutionContext,
                      clean_quali_data: pd.DataFrame):
    df = clean_quali_data
    df['LOAD_TS'] = datetime.datetime.now()

    mess = f"Loading {len(df)} rows of data into SESSION.QUALIFYING_RESULTS"

    context.log.info(mess)

    dis = DiscordUtils()
    dis.send_message(message=mess)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df),
                      'Load Time': str(datetime.datetime.now())}
                  )
