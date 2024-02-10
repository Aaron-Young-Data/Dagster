import datetime

import fastf1
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster_project.utils.discord_utils import DiscordUtils
from dagster_project.fast_f1_functions.collect_data import *
from sklearn.linear_model import LinearRegression
import os

get_data = GetData()
clean = CleanData()
col_list = ['DriverNumber',
            'LapTime',
            'Sector1Time',
            'Sector2Time',
            'Sector3Time',
            'Compound',
            'AirTemp',
            'Rainfall',
            'TrackTemp',
            'WindDirection',
            'WindSpeed']

data_loc = os.getenv('DATA_STORE_LOC')


@asset(config_schema={'event_type': str, 'event_name': str, 'year': int})
def session_info(context):
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
def get_new_session_data(context, session_info: dict):
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
        session_df = clean.time_cols_to_seconds(column_names=['LapTime',
                                                              'Sector1Time',
                                                              'Sector2Time',
                                                              'Sector3Time'],
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
def clean_data(context, get_new_session_data: pd.DataFrame):
    df = get_new_session_data
    fp_col_data = {'fp1_cols': [s + 'FP1' for s in col_list[1:]],
                   'fp2_cols': [s + 'FP2' for s in col_list[1:]],
                   'fp3_cols': [s + 'FP3' for s in col_list[1:]]}

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

    for col_num in range(len(fp_col_data['fp1_cols'])):
        df[fp_col_data['fp1_cols'][col_num]].fillna(value=df[fp_col_data['fp2_cols'][col_num]], inplace=True)
        df[fp_col_data['fp1_cols'][col_num]].replace(to_replace=0, value=df[fp_col_data['fp1_cols'][col_num]].mean(),
                                                     inplace=True)

    context.log.info(
        'If this equals zero its a sprint weekend: ' + str(df['LapTimeFP2'].sum() + df['LapTimeFP3'].sum()))
    if df['LapTimeFP2'].sum() + df['LapTimeFP3'].sum() == 0:
        df.fillna(value=0, inplace=True)
    else:
        df.dropna(subset=fp_col_data['fp2_cols'] + fp_col_data['fp3_cols'], how='all', inplace=True)

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  }
                  )


@asset()
def add_track_data(context, clean_data: pd.DataFrame, session_info: dict):
    event_name = session_info['event_name']
    track_df = pd.read_csv(f'{data_loc}track_data.csv')
    track_info = track_df[track_df['event_name'] == event_name]
    for col in track_info.columns[1:]:
        clean_data[col] = track_info[col].iloc[0]
    return Output(value=clean_data,
                  metadata={
                      'Markdown': MetadataValue.md(clean_data.head().to_markdown()),
                      'Rows': len(clean_data)
                  }
                  )


@asset()
def create_prediction(context, add_track_data: pd.DataFrame):
    lr = LinearRegression()
    data = pd.read_csv(f'{data_loc}data_cleaned.csv')
    y = data['LapTimeQ']
    x = data.drop('LapTimeQ', axis=1)
    lr.fit(x, y)
    predict_df = pd.DataFrame()
    for i in range(len(add_track_data)):
        vals = pd.DataFrame()
        temp = add_track_data.iloc[i].to_frame().transpose()
        idx = temp.index[0]
        predicted_time = lr.predict(temp.drop(['DriverNumber'], axis=1))
        vals['drv_no'] = temp['DriverNumber']
        vals['predicted_time'] = predicted_time
        predict_df = pd.concat([predict_df, vals])

    predict_df = predict_df.sort_values('predicted_time')
    predict_df['Predicted_POS'] = predict_df['predicted_time'].rank(method='first')

    return Output(value=predict_df,
                  metadata={
                      'Markdown': MetadataValue.md(predict_df.head().to_markdown()),
                      'Rows': len(predict_df)
                  }
                  )


@asset()
def send_discord(context,
                 get_new_session_data: pd.DataFrame,
                 create_prediction: pd.DataFrame,
                 add_track_data: pd.DataFrame,
                 session_info: dict):
    if len(get_new_session_data) > 20:
        drivers_replaced = get_new_session_data[get_new_session_data['LapTimeFP1'].isna()]['DriverNumber'].to_list()
        drivers_replaced = tuple(drivers_replaced)
    else:
        drivers_replaced = None

    if len(add_track_data) < 20:
        missing_drivers = -(len(add_track_data) - 20)
    else:
        missing_drivers = 'no'

    year = session_info['year']
    event_name = session_info['event_name']
    justify = 'left'

    output_df = pd.DataFrame()
    output_df['Driver'] = create_prediction['drv_no'].astype(int)
    output_df['Predicted Position'] = create_prediction['Predicted_POS'].astype(int)
    output_df['Predicted Time'] = create_prediction['predicted_time'].astype(float).round(3)

    context.log.info(output_df.to_string(index=False, justify='left'))
    dis = DiscordUtils()
    dis.send_message(message=f'New prediction is available for {year} - {event_name}!\n'
                             f'These drivers where replaced in FP1: {drivers_replaced}\n'
                             f'There are {missing_drivers} missing drivers.\n'
                             f'Prediction:\n\n{output_df.to_string(index=False, justify=justify, col_space=20)}')
    return
