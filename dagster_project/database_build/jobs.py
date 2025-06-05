from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.tables.weather_data import *
from .assets.dim_tables.dim_track_event import *
from .assets.dim_tables.dim_track import *
from .assets.tables.session_data import *
from .assets.tables.f1_calender import *
from .assets.views.dim_event import *
from .assets.dim_tables.dim_weather_type import *
from .assets.views.session_data import *
from .assets.tables.prediction_data import *
from .assets.views.dim_year import *
from .assets.dim_tables.dim_driver import *
from .assets.dim_tables.dim_constructor import *
from .assets.dim_tables.dim_session import *

from .partitions import daily_partitions

rebuild_database_job = define_asset_job("rebuild_database_job",
                                        selection=AssetSelection.assets(create_dim_track,
                                                                        create_dim_track_event,
                                                                        create_dim_weather_type,
                                                                        create_f1_calender,
                                                                        create_prediction_data,
                                                                        create_practice_results_data,
                                                                        create_qualifying_results_data,
                                                                        create_race_results_data,
                                                                        create_weather_forcast,
                                                                        create_dim_event_view,
                                                                        create_cleaned_practice_session_data,
                                                                        create_dim_year_view,
                                                                        create_dim_driver,
                                                                        create_dim_constructor,
                                                                        create_dim_session,
                                                                        create_race_laps_data),
                                        description="Rebuild the database tables and views")
