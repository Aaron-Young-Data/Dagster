from dagster import load_assets_from_package_module
from . import f1_predictor

F1_PREDICTOR = "f1_predictor"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)