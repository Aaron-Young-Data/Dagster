from dagster import load_assets_from_package_module, load_assets_from_modules
from .database import *

DATABASE = "database"
core_database_assets = load_assets_from_package_module(package_module=database,
                                                       group_name=DATABASE)
