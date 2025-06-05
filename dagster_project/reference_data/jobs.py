from dagster import (
    AssetSelection,
    define_asset_job,
    RetryPolicy
)

# Dim Table Assets
from .assets.api.calender import *
from .assets.file.compound import *
from .assets.api.track_data import *
from .assets.api.driver import *
from .assets.api.constructors import *
from .assets.file.session import *

from datetime import datetime

first_year = 2018
last_year = datetime.today().year
year_list = [i for i in range(first_year, last_year + 1, 1)]

# Dim Table Jobs
update_driver_jobs = define_asset_job('update_driver_jobs',
                                      selection=AssetSelection.assets(get_driver_data_api,
                                                                      clean_driver_data,
                                                                      driver_data_to_sql),
                                      description="Job to update the dim driver table",
                                      op_retry_policy=RetryPolicy(max_retries=3)
                                      )

update_constructors_jobs = define_asset_job('update_constructors_jobs',
                                            selection=AssetSelection.assets(get_constructor_data_api,
                                                                            clean_constructor_data,
                                                                            constructor_data_to_sql),
                                            description="Job to update the dim constructor table",
                                            op_retry_policy=RetryPolicy(max_retries=3)
                                            )

update_calender_job = define_asset_job("update_calender_job",
                                       selection=AssetSelection.assets(get_calender_data,
                                                                       calender_to_csv, calender_to_sql),
                                       description="Job to update the current years F1 calender")

update_dim_session_job = define_asset_job("update_dim_session_job",
                                          selection=AssetSelection.assets(get_dim_session_data,
                                                                          dim_session_to_sql),
                                          description="Job to update the dim session data table")

track_data_load_job = define_asset_job('load_track_data_job',
                                       selection=AssetSelection.assets(get_track_data_csv,
                                                                       get_track_data_api,
                                                                       track_data_to_sql),
                                       description='Job to load the track data into MySQL (dim_track)',
                                       op_retry_policy=RetryPolicy(max_retries=3))

track_event_data_load_job = define_asset_job('track_event_data_load_job',
                                             selection=AssetSelection.assets(get_track_event_data_api,
                                                                             track_event_data_to_sql),
                                             description='Job to load the track event data into MySQL (dim_event_track)',
                                             op_retry_policy=RetryPolicy(max_retries=3))

compound_data_load_job = define_asset_job('load_compound_data_job',
                                          selection=AssetSelection.assets(get_compound_data,
                                                                          compound_to_sql),
                                          description='Job to load the compound data into MySQL (dim_compound)')
