import json
from dagster import asset, Output, MetadataValue
import os
import pandas as pd
from resources.sql_io_manager import MySQLDirectConnection
from utils.file_utils import FileUtils
from datetime import datetime, timedelta, date
import urllib
from f1_predictor_data.partitions import weekly_partitions
import openmeteo_requests
import requests_cache
from retry_requests import retry

weather_data_key = os.getenv('WEATHER_DATA_KEY')
data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


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

    context.log.info('Getting forecast for: {} to {}'.format(forecast_date, forecast_date + timedelta(days=2)))

    api_url = "https://api.open-meteo.com/v1/forecast"

    location_df = get_calender_locations_sql

    weather_data = pd.DataFrame()

    for location in location_df.FCST_LOCATION.unique():#
        context.log.info('Getting data for {}'.format(location))

        latitude = location_df[location_df['FCST_LOCATION'] == location].LATITUDE.iloc[0]
        longitude = location_df[location_df['FCST_LOCATION'] == location].LONGITUDE.iloc[0]

        params = {
            "latitude": [latitude],
            "longitude": [longitude],
            "hourly": ["temperature_2m", "precipitation_probability", "precipitation", "weather_code", "cloud_cover",
                       "wind_speed_10m", "wind_direction_10m"],
            "timezone": "GMT",
            "start_date": str(forecast_date),
            "end_date": str(forecast_date + timedelta(days=3))
        }

        responses = openmeteo.weather_api(api_url, params=params)

        response = responses[0]

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_precipitation_probability = hourly.Variables(1).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(6).ValuesAsNumpy()

        hourly_data = {"utc_datetime": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        hourly_data['FCST_LOCATION'] = location
        hourly_data['source'] = 'openmeteo'
        hourly_data["temp"] = hourly_temperature_2m
        hourly_data["precipprob"] = hourly_precipitation_probability
        hourly_data["precip"] = hourly_precipitation
        hourly_data["conditions"] = hourly_weather_code
        hourly_data["cloudcover"] = hourly_cloud_cover
        hourly_data["windspeed"] = hourly_wind_speed_10m
        hourly_data["winddir"] = hourly_wind_direction_10m

        hourly_dataframe = pd.DataFrame(data=hourly_data)

        weather_data = pd.concat((weather_data, hourly_dataframe))

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
       key_prefix=['ml_project_prod', 'WEATHER_FORECAST', 'append'],
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
                       'conditions': 'WEATHER_TYPE_CD',
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
