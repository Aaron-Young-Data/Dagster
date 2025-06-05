from dagster import asset, Output, MetadataValue
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
import os

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset()
def create_practice_results_data(context):
    query = FileUtils.file_to_query('create_practice_data_table')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )

@asset()
def create_qualifying_results_data(context):
    query = FileUtils.file_to_query('create_qualifying_data_table')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )

@asset()
def create_race_results_data(context):
    query = FileUtils.file_to_query('create_race_data_table')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )

@asset()
def create_race_laps_data(context):
    query = FileUtils.file_to_query('create_race_laps_data_table')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )