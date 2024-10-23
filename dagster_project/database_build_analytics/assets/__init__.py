from dagster import load_assets_from_package_module, load_assets_from_modules
from .tables import *
from .dim_tables import *
from .views import *

DIM_TABLES = "dim_tables"
dim_table_assets = load_assets_from_package_module(package_module=dim_tables,
                                                   group_name=DIM_TABLES)

TABLES = "tables"
table_assets = load_assets_from_package_module(package_module=tables,
                                               group_name=TABLES)

VIEWS = "views"
view_assets = load_assets_from_package_module(package_module=views,
                                              group_name=VIEWS)
