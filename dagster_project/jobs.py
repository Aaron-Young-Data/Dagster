from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.ML_project.data_update.session import *
from .assets.ML_project.data_update.calender import *
from .assets.ML_project.data_update.track_data import *
from .assets.ML_project.data_update.compound import *
from .assets.ML_project.data_update.weather_forcast import *
from .assets.data_analysis.data_load.session import *
from .assets.data_analysis.data_download.all_session_data import *
from .assets.data_analysis.data_load.track_status import *
from .partitions import daily_partitions

create_prediction_job = define_asset_job("F1_prediction_job",
                                         selection=AssetSelection.groups(F1_PREDICTOR),
                                         description="Job to predict the f1 qualifying and output to discord",
                                         config={'ops':
                                                     {'session_info':
                                                          {"config":
                                                               {'event_type': 'conventional',
                                                                'event_name': 'Abu Dhabi Grand Prix',
                                                                'year': 2023
                                                                }}}})

evaluate_prediction_job = define_asset_job('evaluation_prediction_job',
                                           selection=AssetSelection.groups(F1_PREDICTOR_EVAL),
                                           description='Job to evaluate the performance of the prediction that was made'
                                                       ' and output to discord',
                                           config={'ops':
                                                       {'quali_session_info':
                                                            {"config":
                                                                 {'event_name': 'Abu Dhabi Grand Prix',
                                                                  'year': 2023
                                                                  }}}})

update_calender_job = define_asset_job("update_calender_job",
                                       selection=AssetSelection.assets(get_calender_data,
                                                                       calender_to_csv, calender_to_sql),
                                       description="Job to update the current years F1 calender")

session_data_load_job = define_asset_job("session_data_load_job",
                                         selection=AssetSelection.assets(get_session_data,
                                                                         session_data_to_sql),
                                         description="Job to pull of session data for a list of years (2018+)"
                                                     " and upload the data to MySQL and CSV",
                                         config={'ops':
                                                     {'get_session_data':
                                                          {"config":
                                                               {'year_list': [2018, 2019, 2020, 2021, 2022, 2023]
                                                                }}}})

weekend_session_data_load_job = define_asset_job("weekend_session_data_load_job",
                                                 selection=AssetSelection.assets(get_session_data_weekend,
                                                                                 session_data_to_sql_append),
                                                 description="Job to upload the selected weekend data to MySQL",
                                                 config={'ops':
                                                             {'get_session_data_weekend':
                                                                  {"config":
                                                                       {'event_type': 'conventional',
                                                                        'event_name': 'Abu Dhabi Grand Prix',
                                                                        'year': 2023
                                                                        }}}})

track_data_load_job = define_asset_job('load_track_data_job',
                                       selection=AssetSelection.assets(get_track_data_csv,
                                                                       track_data_to_sql,
                                                                       dim_track_data_to_sql),
                                       description='Job to load the track data into MySQL (track_data, dim_track)')

compound_data_load_job = define_asset_job('load_compound_data_job',
                                          selection=AssetSelection.assets(get_compound_data,
                                                                          compound_to_sql),
                                          description='Job to load the compound data into MySQL (dim_compound)')

weather_forecast_data_load_job = define_asset_job('load_weather_forcast_data_job',
                                                  selection=AssetSelection.assets(get_calender_locations_sql,
                                                                                  get_weather_forcast_data,
                                                                                  weather_forcast_to_sql),
                                                  description='Job to upload the weather forcast',
                                                  partitions_def=daily_partitions)

load_data_analysis_data_job = define_asset_job('load_data_analysis_data_job',
                                               selection=AssetSelection.assets(get_data_analysis_session_data,
                                                                               data_analysis_session_data_to_sql),
                                               description='Job to load the session data into MySql '
                                                           '(tableau_data.all_session_data)',
                                               config={'ops':
                                                           {'get_data_analysis_session_data':
                                                                {"config":
                                                                     {'year_list': [2018,
                                                                                    2019,
                                                                                    2020,
                                                                                    2021,
                                                                                    2022,
                                                                                    2023,
                                                                                    2024]
                                                                      }}}})

download_all_session_data_job = define_asset_job('download_all_session_data_job',
                                                 selection=AssetSelection.assets(all_session_data_from_sql,
                                                                                 all_session_data_to_csv),
                                                 description='Job to download all of the session data')

load_track_status_data_job = define_asset_job('load_track_status_data_job',
                                              selection=AssetSelection.assets(get_track_status_data_csv,
                                                                              track_status_data_to_sql),
                                              description='Job to load the dim track status table')