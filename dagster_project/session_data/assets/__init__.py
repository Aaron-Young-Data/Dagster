from dagster import load_assets_from_package_module, load_assets_from_modules
from .full_session import *
from .session import *
from .pre_assets import *

FULL_SESSION_UPDATE = 'full_session_update'
full_session_update_assets = load_assets_from_package_module(package_module=full_session,
                                                             group_name=FULL_SESSION_UPDATE)

SESSION_UPDATE = 'session_update'
session_update_assets = load_assets_from_package_module(package_module=session,
                                                        group_name=SESSION_UPDATE)

PRE_ASSETS = 'pre_assets'
pre_assets = load_assets_from_package_module(package_module=pre_assets,
                                             group_name=PRE_ASSETS)
