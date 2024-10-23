from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.tables.weather_data import *
from .assets.dim_tables.dim_track_event import *
from .assets.dim_tables.dim_track import *
from .assets.dim_tables.dim_location import *
from .assets.tables.session_data import *
from .assets.tables.f1_calender import *
from .assets.views.dim_event import *
from .assets.dim_tables.dim_compound import *
from .assets.dim_tables.dim_weather_type import *
from .assets.views.weather_data import *
from .assets.views.session_data import *
from .partitions import daily_partitions

create_weather_forcast_table_job = define_asset_job("create_weather_forcast_table_job",
                                                    selection=AssetSelection.assets(create_weather_forcast_prod),
                                                    description="Create weather forcast data table")

create_weather_forcast_view_job = define_asset_job("create_weather_forcast_view_job",
                                                    selection=AssetSelection.assets(create_weather_forcast_dev),
                                                    description="Create weather forcast data view")

create_dim_location_table_job = define_asset_job("create_dim_location_table_job",
                                                    selection=AssetSelection.assets(create_dim_location),
                                                    description="Create dim location data table")

create_dim_weather_type_table_job = define_asset_job("create_dim_weather_type_table_job",
                                                    selection=AssetSelection.assets(create_dim_weather_type),
                                                    description="Create dim location data table")


create_dim_track_table_job = define_asset_job("create_dim_track_table_job",
                                               selection=AssetSelection.assets(create_dim_track),
                                               description="Create dim track data table")

create_dim_track_event_table_job = define_asset_job("create_dim_track_event_table_job",
                                               selection=AssetSelection.assets(create_dim_track_event),
                                               description="Create dim track event data table")

create_session_data_table_job = define_asset_job("create_session_data_table_job",
                                                     selection=AssetSelection.assets(create_session_data),
                                                     description="Create session data table")

create_cleaned_session_data_view_job = define_asset_job("create_cleaned_session_data_view_job",
                                                     selection=AssetSelection.assets(create_cleaned_session_data),
                                                     description="Create cleaned session data view")

create_f1_calender_table_job = define_asset_job("create_f1_calender_table_job",
                                                selection=AssetSelection.assets(create_f1_calender),
                                                description="Create f1 calender table")

create_dim_event_view_job = define_asset_job("create_dim_event_view_job",
                                              selection=AssetSelection.assets(create_dim_event_view),
                                              description="Create dim event table")

create_dim_compound_table_job = define_asset_job("create_dim_compound_table_job",
                                                 selection=AssetSelection.assets(create_dim_compound),
                                                 description="Create dim compound table")
