from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.dim_tables.calender import *
from .assets.dim_tables.compound import *
from .assets.dim_tables.track_data import *
from .assets.session_data.session import *
from .assets.weather_data.weather_forecast import *
from .assets.dim_tables.weather_type import *
from .partitions import weekly_partitions

first_year = 2018
last_year = datetime.today().year
year_list = [i for i in range(first_year, last_year + 1, 1)]

update_calender_job = define_asset_job("update_calender_job",
                                       selection=AssetSelection.assets(get_calender_data,
                                                                       calender_to_csv, calender_to_sql),
                                       description="Job to update the current years F1 calender")

weather_type_load_job = define_asset_job("weather_type_load_job",
                                         selection=AssetSelection.assets(get_weather_type_csv,
                                                                         weather_type_to_sql),
                                         description="Job to update the dim weather type table")

full_session_data_load_job = define_asset_job("full_session_data_load_job",
                                              selection=AssetSelection.assets(get_full_session_data,
                                                                              full_session_data_to_sql),
                                              description="Job to pull of session data for a list of years (2018+)"
                                                          " and upload the data to MySQL and CSV",
                                              config={'ops':
                                                          {'get_full_session_data':
                                                               {"config":
                                                                    {'year_list': year_list
                                                                     }}}})

session_data_load_job = define_asset_job("session_data_load_job",
                                         selection=AssetSelection.assets(get_session_data,
                                                                         session_data_to_sql),
                                         description="Job to upload the selected weekend data to MySQL",
                                         config={'ops':
                                                     {'get_session_data':
                                                          {"config":
                                                               {'session': 'Practice 1',
                                                                'event_name': 'Abu Dhabi Grand Prix',
                                                                'year': 2023
                                                                }}}})

track_data_load_job = define_asset_job('load_track_data_job',
                                       selection=AssetSelection.assets(get_track_data_csv,
                                                                       get_track_data_api,
                                                                       track_data_to_sql),
                                       description='Job to load the track data into MySQL (dim_track)')

track_event_data_load_job = define_asset_job('track_event_data_load_job',
                                       selection=AssetSelection.assets(get_track_event_data_api,
                                                                       track_event_data_to_sql),
                                       description='Job to load the track event data into MySQL (dim_event_track)')

compound_data_load_job = define_asset_job('load_compound_data_job',
                                          selection=AssetSelection.assets(get_compound_data,
                                                                          compound_to_sql),
                                          description='Job to load the compound data into MySQL (dim_compound)')

weather_forecast_data_load_job = define_asset_job('load_weather_forcast_data_job',
                                                  selection=AssetSelection.assets(get_calender_locations_sql,
                                                                                  get_weather_forecast_data,
                                                                                  weather_forecast_to_sql),
                                                  description='Job to upload the weather forcast',
                                                  partitions_def=weekly_partitions)
