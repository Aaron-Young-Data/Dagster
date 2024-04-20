from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.f1_predictor.weather_data import *
from .assets.f1_predictor.track_data import *
from .assets.f1_predictor.raw_session_data import *
from .assets.f1_predictor.f1_calender import *
from .assets.f1_predictor.dim_event import *
from .assets.f1_predictor.dim_compound import *
from .assets.f1_predictor.cleaned_session_data import *
from .assets.data_analytics.create_analytics_all_session_data import *
from .assets.data_analytics.create_dim_track_status import *
from .partitions import daily_partitions

create_weather_forcast_table_job = define_asset_job("create_weather_forcast_table_job",
                                                    selection=AssetSelection.assets(create_weather_forcast),
                                                    description="Create weather forcast data table")

create_track_data_table_job = define_asset_job("create_track_data_table_job",
                                               selection=AssetSelection.assets(create_track_data),
                                               description="Create track data table")

create_raw_session_data_table_job = define_asset_job("create_raw_session_data_table_job",
                                                     selection=AssetSelection.assets(create_raw_session_data),
                                                     description="Create raw session data table")

create_f1_calender_table_job = define_asset_job("create_f1_calender_table_job",
                                                selection=AssetSelection.assets(create_f1_calender),
                                                description="Create f1 calender table")

create_dim_event_table_job = define_asset_job("create_dim_event_table_job",
                                              selection=AssetSelection.assets(create_dim_event),
                                              description="Create dim event table")

create_dim_compound_table_job = define_asset_job("create_dim_compound_table_job",
                                                 selection=AssetSelection.assets(create_dim_compound),
                                                 description="Create dim compound table")

create_cleaned_session_data_view_job = define_asset_job("create_cleaned_session_data_view_job",
                                                        selection=AssetSelection.assets(create_cleaned_session_data),
                                                        description="Create cleaned session data view")

create_all_session_data_job_analytics = define_asset_job("create_all_session_data_job_analytics",
                                                         selection=AssetSelection.assets(
                                                             create_analytics_all_session_data),
                                                         description="Create all session data table (analytics)")

create_dim_track_status_table_job = define_asset_job("create_dim_track_status_table_job",
                                                     selection=AssetSelection.assets(create_dim_track_status),
                                                     description="Create dim track status table (analytics)")
