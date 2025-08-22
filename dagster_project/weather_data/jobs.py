from dagster import (
    AssetSelection,
    define_asset_job,
    RetryPolicy
)
from .partitions import weekly_partitions
from .assets import *

from .assets.weather.forecast import *
from .assets.reference.weather_type import *
from .assets.weather.historic import *

from datetime import datetime

first_year = 2018
last_year = datetime.today().year
year_list = [i for i in range(first_year, last_year + 1, 1)]

# Weather Forecast Jobs
weather_forecast_data_load_job = define_asset_job('load_weather_forecast_data_job',
                                                  selection=AssetSelection.assets(get_calender_locations_sql,
                                                                                  get_weather_forecast_data,
                                                                                  weather_forecast_cleanup,
                                                                                  weather_forecast_to_sql),
                                                  description='Job to upload the weather forecast')

# Weather Historic Jobs
load_full_weather_historic_data_job = define_asset_job('load_full_weather_historic_data_job',
                                                       selection=AssetSelection.assets(
                                                           get_calender_locations_sql_historic,
                                                           get_full_weather_historic_data,
                                                           full_weather_historic_to_sql
                                                       ),
                                                       description='Job to upload all the historic weather',
                                                       op_retry_policy=RetryPolicy(max_retries=3)
                                                       )

# Weather Historic Jobs
load_weather_historic_data_job = define_asset_job('load_weather_historic_data_job',
                                                       selection=AssetSelection.assets(
                                                           get_calender_locations_sql_historic,
                                                           get_weather_historic_data,
                                                           weather_historical_cleanup,
                                                           weather_historic_to_sql
                                                       ),
                                                       description='Job to upload the partition date historic weather',
                                                       op_retry_policy=RetryPolicy(max_retries=3)
                                                       )

# Reference Jobs
weather_type_load_job = define_asset_job("weather_type_load_job",
                                         selection=AssetSelection.assets(get_weather_type_csv,
                                                                         weather_type_to_sql),
                                         description="Job to update the dim weather type table")
