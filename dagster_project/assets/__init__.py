from dagster import load_assets_from_package_module, load_assets_from_modules
from .ML_project import f1_predictor, data_update, datebase_build, predictor_evaluation
from .data_analysis import data_load, data_download, database_build_analyitcs

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)

F1_PREDICTOR_EVAL = 'predictor_evaluation'
f1_predictor_evaluation_assets = load_assets_from_package_module(package_module=predictor_evaluation,
                                                                 group_name=F1_PREDICTOR_EVAL)
DATA_UPDATE = "data_update"
data_update_assets = load_assets_from_package_module(package_module=data_update, group_name=DATA_UPDATE)

DATABASE_BUILD = "database_build"
database_build_assets = load_assets_from_package_module(package_module=datebase_build, group_name=DATABASE_BUILD)

DATA_ANALYSIS_DATA_LOAD = "data_analysis_load"
data_analysis_load_assets = load_assets_from_package_module(package_module=data_load,
                                                            group_name=DATA_ANALYSIS_DATA_LOAD)

DATA_ANALYSIS_DATA_DOWNLOAD = "data_analysis_download"
data_analysis_download_assets = load_assets_from_package_module(package_module=data_download,
                                                                group_name=DATA_ANALYSIS_DATA_DOWNLOAD)

DATA_ANALYSIS_DATABASE_BUILD = "data_analysis_database_build"
data_analysis_database_build = load_assets_from_package_module(package_module=database_build_analyitcs,
                                                               group_name=DATA_ANALYSIS_DATABASE_BUILD)
