from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
import os
import tensorflow as tf
from keras.src.models.sequential import Sequential
import pandas as pd
from utils.file_utils import FileUtils
from resources.sql_io_manager import MySQLDirectConnection
from utils.discord_utils import DiscordUtils
from fast_f1_functions.collect_data import *

get_data = GetData()
clean = CleanData()
col_list = ['DriverNumber',
            'LapTime',
            'Compound',
            'AirTemp',
            'Rainfall',
            'TrackTemp']

fp_col_data = {'fp1_cols': [s + 'FP1' for s in col_list[1:]],
               'fp2_cols': [s + 'FP2' for s in col_list[1:]],
               'fp3_cols': [s + 'FP3' for s in col_list[1:]]}

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def load_dnn_model(context, save_dnn_model: str):
    save_loc = save_dnn_model
    dnn_model = tf.keras.models.load_model(save_loc)

    return Output(
        value=dnn_model,
        metadata={
            'Save Location': save_loc
        }
    )


@asset(config_schema={'event_type': str, 'event_name': str, 'year': int})
def dnn_model_session_info(context):
    event_type = context.op_config['event_type']
    event_name = context.op_config['event_name']
    year = context.op_config['year']
    return Output(value={'event_type': event_type,
                         'event_name': event_name,
                         'year': year},
                  metadata={'event_type': event_type,
                            'event_name': event_name,
                            'year': year}
                  )


@asset()
def dnn_model_weather_forcast_from_sql(context, dnn_model_session_info):
    session_info = dnn_model_session_info
    session_data_query = FileUtils.file_to_query('sql_session_datetime')
    session_data_query = session_data_query.replace('{event}', session_info['event_name'])
    session_data_query = session_data_query.replace('{year}', str(session_info['year']))
    con = MySQLDirectConnection(port, database, user, password, server)
    session = con.run_query(query=session_data_query)
    session_type = session['EventFormat'].iloc[0]
    if session_type == 'conventional':
        session_time = session['Session4DateUtc'].iloc[0]
    else:
        session_time = session['Session2DateUtc'].iloc[0]

    context.log.info(session_time)

    weather_data_query = FileUtils.file_to_query('sql_weather_forcast_data')

    weather_data_query = weather_data_query.replace('{event}', session_info['event_name'])
    weather_data_query = weather_data_query.replace('{session_date_time}', str(session_time))
    context.log.info(f'Query to run: \n{weather_data_query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=weather_data_query)

    return Output(
        value=df.head(1),
        metadata={
            'num_records': len(df.head(1)),
            'markdown': MetadataValue.md(df.head(1).to_markdown())
        }
    )


@asset()
def dnn_model_get_track_data_sql(context):
    query = FileUtils.file_to_query('sql_track_data')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def dnn_model_get_new_session_data(context, dnn_model_session_info: dict):
    session_info = dnn_model_session_info
    event_type = session_info['event_type']
    event_name = session_info['event_name']
    year = session_info['year']
    context.log.info(f'Event Type: {event_type} \n Event Name: {event_name} \n Year: {year}')
    if event_type == 'conventional':
        sprint_flag = 0
        session_list = ['FP1', 'FP2', 'FP3']
    else:
        sprint_flag = 1
        session_list = ['FP1']
    all_session_data = pd.DataFrame()
    for session in session_list:
        session_data = get_data.session_data(year=year,
                                             location=event_name,
                                             session=session)
        fastest_laps = get_data.fastest_laps(session_data=session_data)
        needed_data = fastest_laps[col_list]
        session_df = clean.time_cols_to_seconds(column_names=['LapTime'],
                                                dataframe=needed_data)

        session_df = session_df.add_suffix(session)
        session_df = session_df.rename(columns={f'DriverNumber{session}': 'DriverNumber'})
        if all_session_data.empty:
            all_session_data = session_df
        else:
            all_session_data = pd.merge(all_session_data, session_df, on='DriverNumber', how="outer")

    if sprint_flag == 1:
        drivers = all_session_data['DriverNumber'].unique()
        temp_df = pd.DataFrame()
        for session in ['FP2', 'FP3']:
            for col in col_list[1:]:
                temp_df[f'{col}{session}'] = 0
        temp_df['DriverNumber'] = drivers
        all_session_data = pd.merge(all_session_data, temp_df, on='DriverNumber', how="outer")

    all_session_data['is_sprint'] = sprint_flag

    return Output(value=all_session_data,
                  metadata={
                      'Markdown': MetadataValue.md(all_session_data.head().to_markdown()),
                      'Rows': len(all_session_data)
                  }
                  )


@asset()
def dnn_model_clean_data(context, dnn_model_get_new_session_data: pd.DataFrame):
    df = dnn_model_get_new_session_data

    df.replace(to_replace={'SOFT': 1,
                           'MEDIUM': 2,
                           'HARD': 3,
                           'INTERMEDIATE': 4,
                           'WET': 5,
                           'HYPERSOFT': 1,
                           'ULTRASOFT': 2,
                           'SUPERSOFT': 3,
                           'UNKNOWN': 0,
                           'TEST_UNKNOWN': 0
                           }, inplace=True)

    df = df.astype(float)

    context.log.info(
        'If this equals zero its a sprint weekend: ' + str(df['LapTimeFP2'].sum() + df['LapTimeFP3'].sum()))

    df['FP1_Missing_Flag'] = 0
    df['FP2_Missing_Flag'] = 0
    df['FP3_Missing_Flag'] = 0

    if df['LapTimeFP2'].sum() + df['LapTimeFP3'].sum() == 0:
        df.loc[(df['LapTimeFP1'].isnull()), 'FP1_Missing_Flag'] = 1
    else:
        df.loc[(df['LapTimeFP1'].isnull()), 'FP1_Missing_Flag'] = 1
        df.loc[(df['LapTimeFP2'].isnull()), 'FP2_Missing_Flag'] = 1
        df.loc[(df['LapTimeFP3'].isnull()), 'FP3_Missing_Flag'] = 1

    df = df[(df['FP2_Missing_Flag'] == 1) & (df['FP3_Missing_Flag'] == 1)]

    print(df)

    df.fillna(value=0, inplace=True)
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  }
                  )


@asset()
def dnn_model_add_track_data(context, dnn_model_clean_data: pd.DataFrame,
                             dnn_model_session_info: dict,
                             dnn_model_get_track_data_sql: pd.DataFrame):
    event_name = dnn_model_session_info['event_name']
    track_df = dnn_model_get_track_data_sql
    track_info = track_df[track_df['event_name'] == event_name]
    for col in track_info.columns[1:]:
        dnn_model_clean_data[col] = track_info[col].iloc[0]
    return Output(value=dnn_model_clean_data,
                  metadata={
                      'Markdown': MetadataValue.md(dnn_model_clean_data.head().to_markdown()),
                      'Rows': len(dnn_model_clean_data)
                  }
                  )


@asset()
def dnn_model_add_weather_forcast_data(context,
                                       dnn_model_add_track_data: pd.DataFrame,
                                       dnn_model_weather_forcast_from_sql: pd.DataFrame):
    df = dnn_model_add_track_data
    weather_data = dnn_model_weather_forcast_from_sql

    for col in weather_data.columns[1:]:
        df[col] = weather_data[col].iloc[0]

    df = df[['DriverNumber'] +
            fp_col_data['fp1_cols'] +
            ['FP1_Missing_Flag'],
            fp_col_data['fp2_cols'] +
            ['FP2_Missing_Flag'],
            fp_col_data['fp3_cols'] +
            ['FP3_Missing_Flag'],
            ['AirTempQ',
             'RainfallQ',
             'is_sprint',
             'traction',
             'tyre_stress',
             'asphalt_grip',
             'braking',
             'asphalt_abrasion',
             'lateral_force',
             'track_evolution',
             'downforce']]

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  }
                  )


@asset()
def dnn_model_create_prediction(context,
                                dnn_model_add_weather_forcast_data: pd.DataFrame,
                                load_dnn_model: Sequential):
    df = dnn_model_add_weather_forcast_data
    dnn_model = load_dnn_model

    predictions = dnn_model.predict(df, verbose=0).flatten()
