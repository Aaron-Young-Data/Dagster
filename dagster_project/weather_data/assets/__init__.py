from dagster import load_assets_from_package_module, load_assets_from_modules
from .reference import *
from .weather import *

REFERENCE_UPDATE = "reference_update"
dim_table_update_assets = load_assets_from_package_module(package_module=reference,
                                                          group_name=REFERENCE_UPDATE)

WEATHER_UPDATE = 'weather_update'
weather_data_update_assets = load_assets_from_package_module(package_module=weather,
                                                             group_name=WEATHER_UPDATE)
