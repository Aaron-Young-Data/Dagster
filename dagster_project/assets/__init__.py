from dagster import load_assets_from_package_module, load_assets_from_modules
from . ML_project import f1_predictor, data_update
#from . ML_project.data_update import calender, session

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)

DATA_UPDATE = "data_update"
data_update_assets = load_assets_from_package_module(package_module=data_update, group_name=DATA_UPDATE)

