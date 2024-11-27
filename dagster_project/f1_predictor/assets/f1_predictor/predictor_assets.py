import datetime
import fastf1
import pandas as pd
from dagster import asset, Output, MetadataValue
from utils.discord_utils import DiscordUtils
from fast_f1_functions.collect_data import *
from sklearn.linear_model import LinearRegression
import os
from utils.file_utils import FileUtils
from resources.sql_io_manager import MySQLDirectConnection
import matplotlib.pyplot as plt
from pandas.plotting import table

get_data = GetData()
clean = CleanData()

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

    query = f'''
    SELECT
        EVENT_CD
    FROM DIM_EVENT 
    WHERE 
        EVENT_YEAR = {year} 
        AND EVENT_NAME = '{event_name}'
    '''

    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)

    event_cd = df['EVENT_CD'].iloc[0]
    return Output(
        value={'event_type': event_type,
               'event_name': event_name,
               'year': year,
               'event_cd': event_cd
               },
        metadata={'event_type': event_type,
                  'event_name': event_name,
                  'year': year,
                  'event_cd': event_cd
                  }
    )


@asset()
def training_data_from_sql(context, session_info: dict):
    event_cd = session_info['event_cd']

    query = FileUtils.file_to_query('sql_training_data')

    query = query.replace('{event_cd}', str(event_cd))

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
def session_data_from_sql(context, session_info: dict):
    event_cd = session_info['event_cd']

    query = FileUtils.file_to_query('sql_session_data')

    query = query.replace('{event_cd}', event_cd)

    context.log.info(f'Query to run: \n{query}')

    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)

    if df['IS_SPRINT'].iloc[0] == 1:
        df.dropna(subset=['LAPTIME_FP1',
                          'COMPOUND_FP1',
                          'AIR_TEMP_FP1',
                          'RAINFALL_FP1',
                          'TRACK_TEMP_FP1'],
                  how='all',
                  inplace=True)
        df.fillna(value=0, inplace=True)
    else:
        df.dropna(how='any', inplace=True)

    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def create_prediction_model(context, training_data_from_sql: pd.DataFrame):
    df = training_data_from_sql

    lr = LinearRegression()

    y = df['LAPTIME_Q']
    x = df.drop('LAPTIME_Q', axis=1)

    lr.fit(x, y)

    return Output(
        value=lr
    )


@asset()
def create_prediction(context, create_prediction_model: LinearRegression, session_data_from_sql: pd.DataFrame):
    lr = create_prediction_model
    df = session_data_from_sql

    predict_df = pd.DataFrame()
    for i in range(len(df)):
        vals = pd.DataFrame()
        temp = df.iloc[i].to_frame().transpose()
        predicted_time = lr.predict(temp.drop(['DRIVER'], axis=1))
        vals['DRIVER'] = temp['DRIVER']
        vals['predicted_time'] = predicted_time
        predict_df = pd.concat([predict_df, vals])

    predict_df = predict_df.sort_values('predicted_time')
    predict_df['Predicted_POS'] = predict_df['predicted_time'].rank(method='first')

    return Output(
        value=predict_df,
        metadata={
            'Markdown': MetadataValue.md(predict_df.head().to_markdown()),
            'Rows': len(predict_df)
        }
    )

@asset(io_manager_key='sql_io_manager', key_prefix=[database, 'PREDICTION_DATA'])
def prediction_data_to_sql(context, session_info: dict, create_prediction: pd.DataFrame):
    event_cd = session_info['event_cd']
    df = create_prediction

    df.drop(columns=['Predicted_POS'], inplace=True)

    df.loc[:, 'EVENT_CD'] = event_cd

    df[['DRIVER', 'DRIVER_NUMBER']] = df['DRIVER'].str.split('-', n=1, expand=True)

    df.rename(columns={'predicted_time': 'PREDICTED_LAPTIME_Q'}, inplace=True)

    df.loc[:, 'LOAD_TS'] = datetime.datetime.now()

    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )





@asset()
def create_table_img(context, create_prediction: pd.DataFrame, session_info: dict):
    df = create_prediction
    file_name = str(session_info['event_name'].replace(' ', '_')) + '_' + str(session_info['year']) + '.png'

    output_df = pd.DataFrame()
    output_df['Driver'] = df['DRIVER'].astype(str)
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
                 create_prediction: pd.DataFrame,
                 session_info: dict):
    year = session_info['year']
    event_name = session_info['event_name']

    output_df = pd.DataFrame()
    output_df['Driver'] = create_prediction['DRIVER']
    output_df['Predicted Position'] = create_prediction['Predicted_POS'].astype(int)
    output_df['Predicted Time'] = create_prediction['predicted_time'].astype(float).round(3)

    context.log.info(output_df.to_string(index=False, justify='left'))

    dis = DiscordUtils()

    dis.send_message(message=f'New prediction is available for {year} - {event_name}!\n'
                             f'This is not gambling advice!',
                     attachment=[create_table_img])
    return
