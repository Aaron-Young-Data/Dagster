import datetime
import fastf1
import pandas as pd
from dagster import asset, Output, MetadataValue
from dagster_project.utils.discord_utils import DiscordUtils
from dagster_project.fast_f1_functions.collect_data import *
from sklearn.linear_model import LinearRegression
import os
from dagster_project.utils.file_utils import FileUtils
from dagster_project.resources.sql_io_manager import MySQLDirectConnection
import matplotlib.pyplot as plt
from pandas.plotting import table

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

fp_col_data = {'fp1_cols': [s + 'FP1' for s in col_list[1:]],
               'fp2_cols': [s + 'FP2' for s in col_list[1:]],
               'fp3_cols': [s + 'FP3' for s in col_list[1:]]}

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


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
def clean_data_from_sql(context):
    query = FileUtils.file_to_query('sql_clean_data')
    query = query.replace('{year_number}', str(datetime.date.today().year))
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)
    df.dropna(how='any', inplace=True)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def weather_forcast_from_sql(context, session_info):
    session_data_query = FileUtils.file_to_query('sql_sesstion_datetime')
    session_data_query = session_data_query.replace('{event}', session_info['event_name'])
    session_data_query = session_data_query.replace('{year}', str(session_info['year']))
    con = MySQLDirectConnection(port, database, user, password, server)
    session_type = con.run_query(query=session_data_query)['EventFormat'].iloc[0]
    if session_type == 'conventional':
        session_time = con.run_query(query=session_data_query)['Session4DateUtc'].iloc[0]
    else:
        session_time = con.run_query(query=session_data_query)['Session2DateUtc'].iloc[0]

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
def get_track_data_sql(context):
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

    if df['LapTimeFP2'].sum() + df['LapTimeFP3'].sum() == 0:
        df.dropna(subset=fp_col_data['fp1_cols'], how='all', inplace=True)
        df.fillna(value=0, inplace=True)
    else:
        df.dropna(how='any', inplace=True)

    df.fillna(value=0, inplace=True)
    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  }
                  )


@asset()
def add_track_data(context, clean_data: pd.DataFrame, session_info: dict, get_track_data_sql: pd.DataFrame):
    event_name = session_info['event_name']
    track_df = get_track_data_sql
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
def add_weather_forcast_data(context, add_track_data: pd.DataFrame, weather_forcast_from_sql: pd.DataFrame):
    df = add_track_data
    weather_data = weather_forcast_from_sql

    for col in weather_data.columns[1:]:
        df[col] = weather_data[col].iloc[0]

    df = df[['DriverNumber'] +
            fp_col_data['fp1_cols'] +
            fp_col_data['fp2_cols'] +
            fp_col_data['fp3_cols'] +
            ['AirTempQ',
             'RainfallQ',
             'WindDirectionQ',
             'WindSpeedQ',
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
def create_prediction(context, add_weather_forcast_data: pd.DataFrame, clean_data_from_sql: pd.DataFrame):
    lr = LinearRegression()
    data = clean_data_from_sql
    y = data['LapTimeQ']
    x = data.drop('LapTimeQ', axis=1)
    lr.fit(x, y)
    predict_df = pd.DataFrame()
    for i in range(len(add_weather_forcast_data)):
        vals = pd.DataFrame()
        temp = add_weather_forcast_data.iloc[i].to_frame().transpose()
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
def create_table_img(context, create_prediction: pd.DataFrame, session_info: dict):
    df = create_prediction
    file_name = str(session_info['event_name']) + '_' + str(session_info['year']) + '.png'

    output_df = pd.DataFrame()
    output_df['Driver'] = df['drv_no'].astype(int).astype(str)
    output_df['Predicted Position'] = df['Predicted_POS'].astype(int).astype(str)
    output_df['Predicted Time'] = df['predicted_time'].astype(float).round(3)

    save_loc = data_loc + file_name
    fig, ax = plt.subplots()
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)

    ax.table(cellText=output_df.values, colLabels=output_df.columns, loc='center')

    plt.savefig(save_loc)

    return Output(value=save_loc,
                  metadata={
                      'File Name': file_name
                  }
                  )


@asset()
def send_discord(context,
                 create_table_img: str,
                 get_new_session_data: pd.DataFrame,
                 create_prediction: pd.DataFrame,
                 add_track_data: pd.DataFrame,
                 session_info: dict):
    if len(get_new_session_data) > 20:
        drivers_session = get_new_session_data['DriverNumber'].astype(int).to_list()
        drivers_prediction = create_prediction['drv_no'].astype(int).to_list()
        drivers_missing = [x for x in drivers_session if x not in set(drivers_prediction)]
        drivers_missing = tuple(drivers_missing)
    else:
        drivers_missing = None

    if len(add_track_data) < 20:
        missing_drivers = -(len(add_track_data) - len(get_new_session_data))
    else:
        missing_drivers = 'no'

    year = session_info['year']
    event_name = session_info['event_name']

    output_df = pd.DataFrame()
    output_df['Driver'] = create_prediction['drv_no'].astype(int)
    output_df['Predicted Position'] = create_prediction['Predicted_POS'].astype(int)
    output_df['Predicted Time'] = create_prediction['predicted_time'].astype(float).round(3)

    context.log.info(output_df.to_string(index=False, justify='left'))
    dis = DiscordUtils()
    dis.send_message(message=f'New prediction is available for {year} - {event_name}!\n'
                             f'These drivers are missing from the prediction: {drivers_missing}\n'
                             f'There are {missing_drivers} missing drivers.\n',
                     attachment=[create_table_img])
    return
