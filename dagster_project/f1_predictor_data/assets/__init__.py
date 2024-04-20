from dagster import load_assets_from_package_module, load_assets_from_modules
from .dim_tables import calender, compound, track_data
from .session_data import session
from .weather_data import weather_forcast

DIM_TABLE_UPDATE = "dim_table_update"
dim_table_update_assets = load_assets_from_package_module(package_module=dim_tables,
                                                          group_name=DIM_TABLE_UPDATE)

SESSION_DATA_UPDATE = 'session_data_update'
session_data_update_assets = load_assets_from_package_module(package_module=session_data,
                                                             group_name=SESSION_DATA_UPDATE)

WEATHER_DATA_UPDATE = 'weather_data_update'
weather_data_update_assets = load_assets_from_package_module(package_module=weather_data,
                                                             group_name=WEATHER_DATA_UPDATE)
