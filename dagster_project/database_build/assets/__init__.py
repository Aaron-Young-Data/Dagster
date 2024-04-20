from dagster import load_assets_from_package_module, load_assets_from_modules
from .data_analytics import *
from .f1_predictor import *

DATABASE_BUILD_ANALYTICS = "database_build_analytics"
database_build_analytics_assets = load_assets_from_package_module(package_module=data_analytics,
                                                   group_name=DATABASE_BUILD_ANALYTICS)

DATABASE_BUILD = "database_build"
database_build_assets = load_assets_from_package_module(package_module=f1_predictor,
                                                       group_name=DATABASE_BUILD)
