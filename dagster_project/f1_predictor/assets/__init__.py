from dagster import load_assets_from_package_module, load_assets_from_modules
from .predictor import *
from .evaluation import *
from .pre_assets import *

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=predictor,
                                                      group_name=F1_PREDICTOR)

F1_PREDICTOR_EVAL = 'predictor_evaluation'
f1_predictor_evaluation_assets = load_assets_from_package_module(package_module=evaluation,
                                                                 group_name=F1_PREDICTOR_EVAL)

PRE_ASSETS = 'pre_assets'
pre_assets = load_assets_from_package_module(package_module=pre_assets,
                                             group_name=PRE_ASSETS)
