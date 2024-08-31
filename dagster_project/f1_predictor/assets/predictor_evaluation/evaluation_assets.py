import os
import pandas as pd
import seaborn as sns
from utils.discord_utils import DiscordUtils
from dagster import asset, Output, MetadataValue
from fast_f1_functions.collect_data import *
import fastf1.plotting
import matplotlib.pyplot as plt
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils


get_data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'event_name': str, 'year': int})
def quali_session_info(context):
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
        value={'event_name': event_name,
               'year': year,
               'event_cd': event_cd
               },
        metadata={'event_name': event_name,
                  'year': year,
                  'event_cd': event_cd
                  }
    )

@asset()
def quali_session_data_from_sql(context, quali_session_info: dict):
    event_cd = quali_session_info['event_cd']

    query = FileUtils.file_to_query('sql_quali_session_data')

    query = query.replace('{event_cd}', str(event_cd))

    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)

    return Output(
        value=df,
        metadata={'Markdown': MetadataValue.md(df.head().to_markdown()),
                  'Rows': len(df)
                  }
    )


@asset()
def evaluate_prediction_dataframe(context, quali_session_data_from_sql: pd.DataFrame, create_prediction: pd.DataFrame):
    quali_df = quali_session_data_from_sql
    predict_df = create_prediction

    quali_df['DRIVER'] = quali_df['DRIVER'].astype(str)
    predict_df['predicted_time'] = predict_df['predicted_time'].astype(float).round(3)

    quali_df.rename({'LAPTIME': 'Actual_LapTime', 'FINAL_POS': 'Actual_POS'}, axis=1, inplace=True)
    predict_df.rename({'predicted_time': 'Predicted_LapTime'}, axis=1, inplace=True)
    df = pd.merge(quali_df, predict_df, on='DRIVER')

    df['LapTime_Dif'] = df['Actual_LapTime'] - df['Predicted_LapTime']
    df['POS_Dif'] = df['Actual_POS'] - df['Predicted_POS']

    return Output(
        value=df,
        metadata={'Markdown': MetadataValue.md(df.head().to_markdown()),
                  'Rows': len(df)
                 }
    )


@asset()
def evaluate_prediction_graph(context, evaluate_prediction_dataframe: pd.DataFrame, quali_session_info: dict):
    event_name = quali_session_info['event_name']
    year = quali_session_info['year']
    file_name = str(quali_session_info['event_name'].replace(' ', '_')) + '_' + str(quali_session_info['year']) + '.png'

    df = evaluate_prediction_dataframe

    fig, ax = plt.subplots(figsize=(8, 8))
    colours = dict()
    for driver in df['DRIVER'].to_list():
        colours.update({driver: fastf1.plotting.driver_color(driver[:3])})

    sns.scatterplot(data=df,
                    x="LapTime_Dif",
                    y="POS_Dif",
                    ax=ax,
                    hue="DRIVER",
                    palette=colours,
                    s=80,
                    linewidth=1,
                    legend='auto',
                    edgecolor='black')

    ax.hlines(y=0, xmin=df['LapTime_Dif'].min(), xmax=df['LapTime_Dif'].max(), color='k', linestyles='dotted')
    ax.vlines(x=0, ymin=df['POS_Dif'].min(), ymax=df['POS_Dif'].max(), color='k', linestyles='dotted')

    plt.suptitle(f"{event_name} {year} Qualifying - Position and LapTime delta (closer to 0,0 the better)\n")

    plt.savefig(data_loc + 'dif_' + file_name)

    fig, ax = plt.subplots(figsize=(8, 8))

    sns.scatterplot(data=df,
                    y="Actual_POS",
                    x="Predicted_POS",
                    ax=ax,
                    hue="DRIVER",
                    palette=colours,
                    s=80,
                    linewidth=1,
                    legend='auto',
                    edgecolor='black')

    ax.plot([0, 20], [0, 20], ls="dotted", c='black')
    ax.invert_yaxis()

    plt.suptitle(f"{event_name} {year} Qualifying - Actual vs Predicted position (Closer to the line better)\n")

    plt.savefig(data_loc + 'vs_' + file_name)

    return Output(value=file_name,
                  metadata={
                      'File Name': file_name
                  }
                  )

@asset()
def send_evaluation_discord(context, evaluate_prediction_graph):
    fig1 = data_loc + 'vs_' + evaluate_prediction_graph
    fig2 = data_loc + 'dif_' + evaluate_prediction_graph
    dis = DiscordUtils()
    dis.send_message(message=f'This is how the model performed:\n'
                             f'*The quali time is based of the drivers fastest laps in the session \n so could be wrong i.e set a faster time in Q2 compared to Q3',
                     attachment=[fig2, fig1])

