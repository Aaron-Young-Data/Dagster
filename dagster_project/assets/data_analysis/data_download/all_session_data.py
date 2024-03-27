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

tableau_data_loc = os.getenv('TABLEAU_DATA_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def all_session_data_from_sql(context):
    query = FileUtils.file_to_query('sql_all_session_data')
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
def all_session_data_to_csv(context, all_session_data_from_sql: pd.DataFrame):
    df = all_session_data_from_sql
    df.to_csv(tableau_data_loc + 'Lap_Data.csv')
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )
