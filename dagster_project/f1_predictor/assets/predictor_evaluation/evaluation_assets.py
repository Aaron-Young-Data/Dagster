import os
import pandas as pd
import seaborn as sns
from utils.discord_utils import DiscordUtils
from dagster import asset, Output, MetadataValue
from fast_f1_functions.collect_data import *
import fastf1.plotting
import matplotlib.pyplot as plt

get_data = GetData()
clean = CleanData()

data_loc = os.getenv('DATA_STORE_LOC')


@asset(config_schema={'event_name': str, 'year': int})
def quali_session_info(context):
    event_name = context.op_config['event_name']
    year = context.op_config['year']
    return Output(value={'event_name': event_name,
                         'year': year},
                  metadata={'event_name': event_name,
                            'year': year}
                  )

@asset()
def get_quali_session_data(context, quali_session_info: dict):
    event_name = quali_session_info['event_name']
    year = quali_session_info['year']
    session_data = get_data.session_data(year=year,
                                         location=event_name,
                                         session='Q')
    fastest_laps = get_data.fastest_laps(session_data=session_data)
    fastest_laps = clean.order_laps_delta(fastest_laps)
    needed_data = fastest_laps[['DriverNumber', 'Driver', 'LapTime', 'Final_POS']]
    session_df = clean.time_cols_to_seconds(column_names=['LapTime'],
                                            dataframe=needed_data)

    return Output(value=session_df,
                  metadata={
                      'Markdown': MetadataValue.md(session_df.head().to_markdown()),
                      'Rows': len(session_df)
                  }
                  )


@asset()
def evaluate_prediction_dataframe(context, get_quali_session_data: pd.DataFrame, create_prediction: pd.DataFrame):
    quali_df = get_quali_session_data
    predict_df = create_prediction

    quali_df['DriverNumber'] = quali_df['DriverNumber'].astype(float)
    predict_df['predicted_time'] = predict_df['predicted_time'].astype(float).round(3)

    quali_df.rename({'LapTime': 'Actual_LapTime', 'Final_POS': 'Actual_POS'}, axis=1, inplace=True)
    predict_df.rename({'predicted_time': 'Predicted_LapTime'}, axis=1, inplace=True)
    df = pd.merge(quali_df, predict_df, left_on='DriverNumber', right_on='drv_no')

    df['LapTime_Dif'] = df['Actual_LapTime'] - df['Predicted_LapTime']
    df['POS_Dif'] = df['Actual_POS'] - df['Predicted_POS']

    return Output(value=df,
                  metadata={
                      'Markdown': MetadataValue.md(df.head().to_markdown()),
                      'Rows': len(df)
                  }
                  )


@asset()
def evaluate_prediction_graph(context, evaluate_prediction_dataframe: pd.DataFrame, quali_session_info: dict):
    event_name = quali_session_info['event_name']
    year = quali_session_info['year']
    file_name = str(quali_session_info['event_name']) + '_' + str(quali_session_info['year']) + '.png'

    df = evaluate_prediction_dataframe

    fig, ax = plt.subplots(figsize=(8, 8))
    colours = dict()
    for driver in df['Driver'].to_list():
        colours.update({driver: fastf1.plotting.driver_color(driver)})

    sns.scatterplot(data=df,
                    x="LapTime_Dif",
                    y="POS_Dif",
                    ax=ax,
                    hue="Driver",
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
                    hue="Driver",
                    palette=colours,
                    s=80,
                    linewidth=1,
                    legend='auto',
                    edgecolor='black')

    ax.plot(ax.get_xlim(), ax.get_ylim(), ls="dotted", c='black')
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

