import datetime
from sklearn.metrics import mean_squared_error
import pandas as pd
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from utils.discord_utils import DiscordUtils
import os
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from keras.src.models.sequential import Sequential
from keras.src.callbacks.history import History
import numpy as np
import matplotlib.pyplot as plt

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def dnn_clean_data_from_sql(context):
    query = FileUtils.file_to_query('sql_dnn_model_clean_data')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=query)
    df = df.astype(float)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@multi_asset(
    outs={'test_data': AssetOut(), 'train_data': AssetOut()}
)
def test_train_split(context, dnn_clean_data_from_sql: pd.DataFrame):
    df = dnn_clean_data_from_sql
    train_data = df.sample(frac=0.8, random_state=0)
    test_data = df.drop(train_data.index)

    yield Output(
        value=train_data,
        metadata={
            'num_records': len(train_data),
            'markdown': MetadataValue.md(df.head().to_markdown())
        },
        output_name='train_data'
    )
    yield Output(
        value=test_data,
        metadata={
            'num_records': len(test_data),
            'markdown': MetadataValue.md(df.head().to_markdown())
        },
        output_name='test_data'
    )


@asset()
def build_dnn_model(context, train_data: pd.DataFrame):
    train_features = train_data.copy()
    train_features.pop('LapTimeQ')

    normalizer = tf.keras.layers.Normalization(axis=-1)
    normalizer.adapt(np.array(train_features))

    dnn_model = keras.Sequential([
        normalizer,
        layers.Dense(32, activation='relu'),
        layers.Dense(16, activation='relu'),
        layers.Dense(1)
    ])

    dnn_model.compile(loss='mean_absolute_error',
                      optimizer=tf.keras.optimizers.Adam(0.001))

    return Output(
        value=dnn_model
    )


@multi_asset(
    outs={'train_dnn_model_history': AssetOut(), 'train_dnn_model': AssetOut()}
)
def train_dnn_model(context, build_dnn_model: Sequential, train_data: pd.DataFrame):
    dnn_model = build_dnn_model

    train_features = train_data.copy()
    train_labels = train_features.pop('LapTimeQ')

    history = dnn_model.fit(
        train_features,
        train_labels,
        validation_split=0.1,
        verbose=0, epochs=1000)

    yield Output(
        value=dnn_model,
        output_name='train_dnn_model'
    )
    yield Output(
        value=history,
        output_name='train_dnn_model_history'
    )


@asset()
def evaluate_dnn_model(context,
                       train_dnn_model_history: History,
                       train_dnn_model: Sequential,
                       test_data: pd.DataFrame):
    save_loc = data_loc + str(datetime.date.today())

    dnn_model = train_dnn_model_history

    test_features = test_data.copy()
    test_labels = test_features.pop('LapTimeQ')

    test_predictions = train_dnn_model.predict(test_features, verbose=0).flatten()

    plt.title('Plot Loss')
    plt.plot(dnn_model.history['loss'], label='loss')
    plt.plot(dnn_model.history['val_loss'], label='val_loss')
    plt.ylim([0, 10])
    plt.xlabel('Epoch')
    plt.ylabel('Error [LapTimeQ]')
    plt.legend()
    plt.grid(True)

    plt.savefig(save_loc + '_plot_loss.png')

    plt.clf()

    a = plt.axes(aspect='equal')
    plt.title('Prediction vs Actual Values')
    plt.scatter(test_labels, test_predictions)
    plt.xlabel('True Values [LapTimeQ]')
    plt.ylabel('Predictions [LapTimeQ]')
    lims = [0, 150]
    plt.xlim(lims)
    plt.ylim(lims)
    _ = plt.plot(lims, lims)

    plt.savefig(save_loc + '_predict_vs_actual.png')

    plt.clf()

    error = test_predictions - test_labels
    plt.hist(error, bins=25)
    plt.xlabel('Prediction Error [LapTimeQ]')
    _ = plt.ylabel('Count')

    plt.savefig(save_loc + '_predict_error_cnt.png')

    y_pred = test_predictions
    y_true = test_labels

    rmse = mean_squared_error(y_true=y_true, y_pred=y_pred)

    output_dic = {'RMSE': rmse,
                  'plot_loss': save_loc + '_plot_loss.png',
                  'predict_vs_actual': save_loc + '_predict_vs_actual.png',
                  'predict_error_count': save_loc + '_predict_error_cnt.png'}

    return Output(value=output_dic,
                  metadata={
                      'File Name (Plot Loss)': save_loc + '_plot_loss.png',
                      'File Name (Predict vs Actual)': save_loc + '_predict_vs_actual.png',
                      'File Name (Predict Error Count)': save_loc + '_predict_error_cnt.png',
                      'RMSE': float(rmse),
                  }
                  )


@asset()
def save_dnn_model(context, train_dnn_model: Sequential):
    dnn_model = train_dnn_model

    save_loc = data_loc + str(datetime.date.today()) + '_dnn_model.keras'

    dnn_model.save(save_loc)

    return Output(
        value=save_loc,
        metadata={
            'Save Location:': save_loc
        }
    )


@asset()
def evaluate_dnn_model_to_discord(context, evaluate_dnn_model: dict, save_dnn_model: str):
    eval_dic = evaluate_dnn_model
    save_loc = save_dnn_model

    dis = DiscordUtils()
    dis.send_message(message=f'New DNN model has been created and is saved as: {save_loc}\n'
                             f'The RMSE value is: {eval_dic["RMSE"]}\n'
                             f'Evaluation graphs are below:\n',
                     attachment=[eval_dic['plot_loss'],
                                 eval_dic['predict_vs_actual'],
                                 eval_dic['predict_error_count']])
