from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.f1_predictor.weather_data import *
from .assets.f1_predictor.dim_track_event import *
from .assets.f1_predictor.dim_track import *
from .assets.f1_predictor.dim_location import *
from .assets.f1_predictor.session_data import *
from .assets.f1_predictor.f1_calender import *
from .assets.f1_predictor.dim_event import *
from .assets.f1_predictor.dim_compound import *
from .assets.data_analytics.create_analytics_all_session_data import *
from .assets.data_analytics.create_dim_track_status import *
from .partitions import daily_partitions

create_weather_forcast_table_job = define_asset_job("create_weather_forcast_table_job",
                                                    selection=AssetSelection.assets(create_weather_forcast_prod),
                                                    description="Create weather forcast data table")

create_dim_location_table_job = define_asset_job("create_dim_location_table_job",
                                                    selection=AssetSelection.assets(create_dim_track),
                                                    description="Create dim location data table")

create_weather_forcast_view_job = define_asset_job("create_weather_forcast_view_job",
                                                    selection=AssetSelection.assets(create_weather_forcast_dev),
                                                    description="Create weather forcast data view")

create_dim_track_table_job = define_asset_job("create_dim_track_table_job",
                                               selection=AssetSelection.assets(create_dim_track),
                                               description="Create track data table")

create_dim_track_event_table_job = define_asset_job("create_dim_track_event_table_job",
                                               selection=AssetSelection.assets(create_dim_track_event),
                                               description="Create track data table")

create_session_data_table_job = define_asset_job("create_session_data_table_job",
                                                     selection=AssetSelection.assets(create_session_data),
                                                     description="Create session data table")

create_f1_calender_table_job = define_asset_job("create_f1_calender_table_job",
                                                selection=AssetSelection.assets(create_f1_calender),
                                                description="Create f1 calender table")

create_dim_event_view_job = define_asset_job("create_dim_event_view_job",
                                              selection=AssetSelection.assets(create_dim_event_view),
                                              description="Create dim event table")

create_dim_compound_table_job = define_asset_job("create_dim_compound_table_job",
                                                 selection=AssetSelection.assets(create_dim_compound),
                                                 description="Create dim compound table")

create_all_session_data_job_analytics = define_asset_job("create_all_session_data_job_analytics",
                                                         selection=AssetSelection.assets(
                                                             create_analytics_all_session_data),
                                                         description="Create all session data table (analytics)")

create_dim_track_status_table_job = define_asset_job("create_dim_track_status_table_job",
                                                     selection=AssetSelection.assets(create_dim_track_status),
                                                     description="Create dim track status table (analytics)")
