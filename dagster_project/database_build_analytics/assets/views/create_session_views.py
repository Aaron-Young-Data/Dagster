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


@asset
def create_practice_results_view(context):
    query = FileUtils.file_to_query('create_analytics_agg_practice_result_vw')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )


@asset
def create_qualifying_results_view(context):
    query = FileUtils.file_to_query('create_analytics_agg_qualifying_result_vw')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )


@asset
def create_race_results_view(context):
    query = FileUtils.file_to_query('create_analytics_agg_race_result_driver_vw')
    context.log.info(f'Query to run: \n{query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query_no_output(query=query)
    return Output(
        value=df
    )