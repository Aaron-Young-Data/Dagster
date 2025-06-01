from dagster import load_assets_from_package_module, load_assets_from_modules
from .full_session import *
from .session import *

FULL_SESSION_UPDATE = 'full_session_update'
full_session_update_assets = load_assets_from_package_module(package_module=full_session,
                                                             group_name=FULL_SESSION_UPDATE)

SESSION_UPDATE = 'session_update'
session_update_assets = load_assets_from_package_module(package_module=session,
                                                        group_name=SESSION_UPDATE)
