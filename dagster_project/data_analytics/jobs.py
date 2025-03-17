from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.data_download.all_session_data import *
from .assets.data_load.all_sessions import *
from .assets.data_load.track_status import *
from .partitions import daily_partitions

download_all_session_data_job = define_asset_job('download_all_session_data_job',
                                                 selection=AssetSelection.assets(all_session_data_from_sql,
                                                                                 all_session_data_to_csv),
                                                 description='Job to download all of the session data')

data_load_all_session_data_job = define_asset_job('data_load_all_session_data_job',
                                                  selection=AssetSelection.assets(get_data_analysis_session_data,
                                                                                  data_analysis_session_data_to_sql),
                                                  description='Job to load all of the session data',
                                                  config={'ops':
                                                              {'get_data_analysis_session_data':
                                                                   {"config":
                                                                        {'year_list': [2018, 2019, 2020, 2021, 2022, 2023, 2024]}
                                                                    }}})

data_load_track_status_data_job = define_asset_job('data_load_track_status_data_job',
                                                   selection=AssetSelection.assets(get_track_status_data_csv,
                                                                                   track_status_data_to_sql),
                                                   description='Job to load the dim track status table')

data_load_weekend_session_data = define_asset_job('data_load_weekend_session_data',
                                                  selection=AssetSelection.assets(
                                                      get_data_analysis_weekend_session_data,
                                                      data_analysis_weekend_session_data_to_sql),
                                                  description='Job to load the weekend data into MySql '
                                                              '(tableau_data.all_session_data)',
                                                  config={'ops':
                                                              {'get_data_analysis_weekend_session_data':
                                                                   {"config":
                                                                        {'event_type': 'conventional',
                                                                         'event_name': 'Abu Dhabi Grand Prix',
                                                                         'year': 2023
                                                                         }}}})
