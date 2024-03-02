from dagster import load_assets_from_package_module, load_assets_from_modules
from . ML_project import f1_predictor, data_update, datebase_build, predictor_evaluation

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)

F1_PREDICTOR_EVAL = 'predictor_evaluation'
f1_predictor_evaluation_assets = load_assets_from_package_module(package_module=predictor_evaluation,
                                                                 group_name=F1_PREDICTOR_EVAL)

DATA_UPDATE = "data_update"
data_update_assets = load_assets_from_package_module(package_module=data_update, group_name=DATA_UPDATE)

DATABASE_BUILD = "database_build"
database_build_assets = load_assets_from_package_module(package_module=datebase_build, group_name=DATABASE_BUILD)
