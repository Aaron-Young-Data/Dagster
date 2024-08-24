from dagster import asset, Output, MetadataValue, multi_asset, AssetOut
import os
import tensorflow as tf
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
    return Output(value={'event_type': event_type,
                         'event_name': event_name,
                         'event_cd': event_cd,
                         'year': year},
                  metadata={'event_type': event_type,
                            'event_name': event_name,
                            'event_cd': event_cd,
                            'year': year}
                  )

@asset()
def dnn_model_session_data_sql(context, dnn_model_session_info):
    event_cd = dnn_model_session_info['event_cd']




