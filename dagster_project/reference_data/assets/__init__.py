from dagster import load_assets_from_package_module
from .file import *
from .api import *

API_UPDATE = "api_update"
api_update_assets = load_assets_from_package_module(package_module=api,
                                                    group_name=API_UPDATE)

FILE_UPDATE = "file_update"
file_update_assets = load_assets_from_package_module(package_module=file,
                                                    group_name=FILE_UPDATE)
