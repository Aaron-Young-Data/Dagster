from dagster import load_assets_from_package_module
from . ML_project import f1_predictor, calender_update

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)

F1_CALENDER_UPDATE = "calender_updator"
calender_update_assets = load_assets_from_package_module(package_module=calender_update, group_name=F1_CALENDER_UPDATE)