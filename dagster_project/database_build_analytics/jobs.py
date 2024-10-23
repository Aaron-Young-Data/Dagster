from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.tables.create_all_session_data import *
from .assets.dim_tables.create_dim_track_status import *
from .assets.views.create_session_views import *
from .assets.views.create_championship_views import *
from .partitions import daily_partitions

create_dim_track_status_table_job = define_asset_job("create_dim_track_status_table_job",
                                                     selection=AssetSelection.assets(create_dim_track_status),
                                                     description="Create dim track status data table")

create_all_session_data_table_job = define_asset_job("create_all_session_data_table_job",
                                                     selection=AssetSelection.assets(
                                                         create_analytics_all_session_data),
                                                     description="Create all session data table")

create_race_results_view_job = define_asset_job("create_race_results_view_job",
                                                selection=AssetSelection.assets(
                                                    create_race_results_view),
                                                description="Create race results view")

create_qualifying_results_view_job = define_asset_job("create_qualifying_results_view_job",
                                                      selection=AssetSelection.assets(
                                                          create_qualifying_results_view),
                                                      description="Create qualifying results view")

create_practice_results_view_job = define_asset_job("create_practice_results_view_job",
                                                    selection=AssetSelection.assets(
                                                        create_practice_results_view),
                                                    description="Create practice results view")

create_constructors_championship_view_job = define_asset_job("create_constructors_championship_view_job",
                                                             selection=AssetSelection.assets(
                                                                 create_constructors_championship_view),
                                                             description="Create constructors championship results view")

create_drivers_championship_view_job = define_asset_job("create_drivers_championship_view_job",
                                                        selection=AssetSelection.assets(
                                                            create_drivers_championship_view),
                                                        description="Create drivers championship results view")
