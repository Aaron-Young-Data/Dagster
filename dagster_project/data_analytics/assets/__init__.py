from dagster import load_assets_from_package_module, load_assets_from_modules
from .data_download import all_session_data
from .data_load import all_sessions, track_status

DATA_LOAD = "data_load"
data_load_assets = load_assets_from_package_module(package_module=data_load,
                                                   group_name=DATA_LOAD)

DATA_DOWNLOAD = "data_download"
data_download_assets = load_assets_from_package_module(package_module=data_download,
                                                       group_name=DATA_DOWNLOAD)
