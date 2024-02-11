import os
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from dagster_project.fast_f1_functions.collect_data import GetData, CleanData

data = GetData()
clean = CleanData()
data_loc = os.getenv('DATA_STORE_LOC')


@asset(config_schema={'year_list': list})
def get_session_data(context):
    full_data = pd.DataFrame()
    year_list = context.op_config['year_list']
    context.log.info(str(year_list))
    for year in year_list:
        context.log.info(year)
        calendar = data.get_calender(year)
        races = calendar['EventName'].to_list()
        for race in races:
            event_type = calendar[calendar['EventName'] == race]['EventFormat'].to_list()[0]
            all_sessions = data.session_list(calendar[calendar['EventName'] == race][['Session1',
                                                                                      'Session2',
                                                                                      'Session3',
                                                                                      'Session4',
                                                                                      'Session5']])
            sessions_practice = [x for x in all_sessions if 'Practice' in x]
            sessions_practice.append('Qualifying')
            sessions = sessions_practice
            if 'Practice' in sessions[len(sessions) - 1]:
                sessions = sessions.pop()
            event_data = pd.DataFrame()
            for session in sessions:
                session_data = data.session_data(year=year, location=race, session=session)
                fastest_laps = data.fastest_laps(session_data=session_data)
                if len(fastest_laps) == 0:
                    break
                fastest_laps_ordered = clean.order_laps_delta(laps=fastest_laps, include_pos=False)
                needed_data = fastest_laps_ordered[['DriverNumber',
                                                    'LapTime',
                                                    'Sector1Time',
                                                    'Sector2Time',
                                                    'Sector3Time',
                                                    'Compound',
                                                    'AirTemp',
                                                    'Rainfall',
                                                    'TrackTemp',
                                                    'WindDirection',
                                                    'WindSpeed']]
                session_df = clean.time_cols_to_seconds(column_names=['LapTime',
                                                                      'Sector1Time',
                                                                      'Sector2Time',
                                                                      'Sector3Time'],
                                                        dataframe=needed_data)

                try:
                    suffix = "FP" + str(int(session[-1:]))
                except:
                    suffix = 'Q'

                if event_data.empty:
                    event_data = session_df.add_suffix(suffix)
                    event_data = event_data.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                else:
                    session_df = session_df.add_suffix(suffix)
                    session_df = session_df.rename(columns={f'DriverNumber{suffix}': 'DriverNumber'})
                    event_data = pd.merge(event_data, session_df, on='DriverNumber', how="outer")
            event_data['event_name'] = race
            event_data['year'] = year
            event_data['event_type'] = event_type
            full_data = pd.concat([full_data, event_data])
    return Output(
        value=full_data,
        metadata={
            'Markdown': MetadataValue.md(full_data.head().to_markdown()),
            'Rows': len(full_data)
        }

    )


@asset()
def session_to_file(context, get_session_data: pd.DataFrame):
    df = get_session_data
    df.to_csv(f'{data_loc}session_data.csv')
    return


@asset()
def get_track_data(context):
    track_data = pd.read_csv(f'{data_loc}track_data.csv')
    return Output(value=track_data,
                  metadata={
                      'Markdown': MetadataValue.md(track_data.head().to_markdown()),
                      'Rows': len(track_data)
                  })


@multi_asset(
    outs={
        "sprint_data": AssetOut(is_required=True), 'conventional_data': AssetOut(is_required=True)
    }
)
def session_data_split(context, get_session_data: pd.DataFrame):
    df = get_session_data
    sprint_data = df[df['event_type'] != 'conventional']
    conventional_data = df[df['event_type'] == 'conventional']
    yield Output(value=sprint_data,
                 metadata={
                     'Markdown': MetadataValue.md(sprint_data.head().to_markdown()),
                     'Rows': len(sprint_data)
                 },
                 output_name='sprint_data')
    yield Output(value=conventional_data,
                 metadata={
                     'Markdown': MetadataValue.md(conventional_data.head().to_markdown()),
                     'Rows': len(conventional_data)
                 },
                 output_name='conventional_data')


@asset()
def clean_sprint_data(context, sprint_data: pd.DataFrame):
    df = sprint_data
    df.fillna(value=0, inplace=True)
    df['is_sprint'] = 1
    df.replace(to_replace={'SOFT': 1,
                           'MEDIUM': 2,
                           'HARD': 3,
                           'INTERMEDIATE': 4,
                           'WET': 5,
                           'HYPERSOFT': 1,
                           'ULTRASOFT': 2,
                           'SUPERSOFT': 3,
                           'UNKNOWN': 0,
                           'TEST_UNKNOWN': 0,
                           'nan': 0,
                           '': 0
                           }, inplace=True)
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  })


@asset()
def clean_conventional_data(context, conventional_data: pd.DataFrame):
    df = conventional_data
    df['is_sprint'] = 0
    df.replace(to_replace={'SOFT': 1,
                           'MEDIUM': 2,
                           'HARD': 3,
                           'INTERMEDIATE': 4,
                           'WET': 5,
                           'HYPERSOFT': 1,
                           'ULTRASOFT': 2,
                           'SUPERSOFT': 3,
                           'UNKNOWN': 0,
                           'TEST_UNKNOWN': 0,
                           'nan': 0,
                           '': 0
                           }, inplace=True)
    df.dropna(how='any', inplace=True)
    df.to_csv('test.csv')
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  })


@asset()
def merge_cleaned_data(context,
                       clean_sprint_data: pd.DataFrame,
                       clean_conventional_data: pd.DataFrame,
                       get_track_data: pd.DataFrame):
    sprint_data = clean_sprint_data
    conventional_data = clean_conventional_data
    df = pd.concat([sprint_data, conventional_data])
    df = pd.merge(df, get_track_data, on='event_name', how="outer")
    df.drop(columns=['DriverNumber',
                     'event_name',
                     'year',
                     'event_type',
                     'Sector1TimeQ',
                     'Sector2TimeQ',
                     'Sector3TimeQ',
                     'CompoundQ',
                     'AirTempQ',
                     'RainfallQ',
                     'TrackTempQ',
                     'WindDirectionQ',
                     'WindSpeedQ'], inplace=True)
    df = df.astype(float)
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  })


@asset()
def cleaned_data_to_file(context, merge_cleaned_data: pd.DataFrame):
    df = merge_cleaned_data
    df.to_csv(f'{data_loc}data_cleaned.csv')
    return
