import json
from dagster import asset, Output, MetadataValue
import os
import pandas as pd
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from datetime import datetime, timedelta, date
import urllib
from f1_predictor_data.partitions import weekly_partitions

weather_data_key = os.getenv('WEATHER_DATA_KEY')
data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(partitions_def=weekly_partitions)
def get_calender_locations_sql(context):
    partition_date_str = context.partition_key
    forcast_date = datetime.strptime(partition_date_str, '%Y-%M-%d')
    year = forcast_date.year
    query = FileUtils.file_to_query('sql_calender_data')
    formatted_query = query.replace('{partitioned_date_year}', str(year))
    context.log.info(f'Query to run: \n{formatted_query}')
    con = MySQLDirectConnection(port, database, user, password, server)
    df = con.run_query(query=formatted_query)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset(partitions_def=weekly_partitions)
def get_weather_forecast_data(context, get_calender_locations_sql: pd.DataFrame):
    partition_date_str = context.partition_key
    forecast_date = datetime.strptime(partition_date_str, '%Y-%m-%d').date()

    context.log.info('Getting forecast for: {}'.format(forecast_date))

    location_df = get_calender_locations_sql

    locations = location_df['FCST_LOCATION'].to_list()

    weather_data = pd.DataFrame()

    for location in locations:
        api_query = ('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{}/{}/{'
                     '}?key={}&unitGroup=metric&include=hours').format(location,
                                                                       str(forecast_date),
                                                                       str(forecast_date + timedelta(days=2)),
                                                                       weather_data_key)

        context.log.info('Running query URL: {}'.format(api_query))

        try:
            data = urllib.request.urlopen(api_query)
        except urllib.error.HTTPError as e:
            err = e.read().decode()
            raise Exception('Error code: {} {}'.format(e.code, err))
        except urllib.error.URLError as e:
            err = e.read().decode()
            raise Exception('Error code: {} {}'.format(e.code, err))

        loc_weather_json = json.loads(data.read().decode('utf-8'))

        loc_weather_df = pd.json_normalize(loc_weather_json['days'][0]['hours'])

        loc_weather_df.loc[:, 'FCST_LOCATION'] = location

        weather_data = pd.concat((weather_data, loc_weather_df))

    weather_data.rename(columns={'datetime': 'time'}, inplace=True)
    weather_data.loc[:, 'date'] = forecast_date
    weather_data.loc[:, 'utc_datetime'] = pd.to_datetime(
        weather_data['date'].astype(str) + ' ' + weather_data['time'].astype(str))

    weather_data = weather_data[['FCST_LOCATION',
                                 'utc_datetime',
                                 'temp',
                                 'precip',
                                 'precipprob',
                                 'windspeed',
                                 'winddir',
                                 'cloudcover',
                                 'conditions',
                                 'source']]

    return Output(
        value=weather_data,
        metadata={
            'num_records': len(weather_data),
            'markdown': MetadataValue.md(weather_data.head(10).to_markdown())
        }
    )


@asset(io_manager_key='sql_io_manager',
       key_prefix=[database, 'weather_forecast', 'append'],
       partitions_def=weekly_partitions)
def weather_forecast_to_sql(context, get_weather_forecast_data: pd.DataFrame):
    load_date = datetime.today()
    df = get_weather_forecast_data
    df.rename(columns={'FCST_LOCATION': 'FCST_LOCATION',
                       'utc_datetime': 'FCST_DATETIME',
                       'temp': 'TEMPERATURE',
                       'precip': 'PRECIPITATION',
                       'precipprob': 'PRECIPITATION_PROB',
                       'windspeed': 'WIND_SPEED',
                       'winddir': 'WIND_DIRECTION',
                       'cloudcover': 'CLOUD_COVER',
                       'conditions': 'CONDITIONS',
                       'source': 'FCST_SOURCE'},
              inplace=True)
    df.loc[:, 'LOAD_TS'] = load_date
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
