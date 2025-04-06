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
from .assets.dim_tables.dim_compound import *
from .assets.dim_tables.dim_weather_type import *
from .assets.views.weather_data import *
from .assets.views.session_data import *
from .assets.tables.prediction_data import *
from .assets.views.dim_year import *
from .partitions import daily_partitions

rebuild_database_job = define_asset_job("rebuild_database_job",
                                        selection=AssetSelection.assets(create_dim_compound,
                                                                        create_dim_track,
                                                                        create_dim_track_event,
                                                                        create_dim_weather_type,
                                                                        create_f1_calender,
                                                                        create_prediction_data,
                                                                        create_session_data,
                                                                        create_weather_forcast_prod,
                                                                        create_dim_event_view,
                                                                        create_weather_forcast_dev,
                                                                        create_cleaned_session_data,
                                                                        create_dim_year_view),
                                        description="Rebuild the database tables and views")
