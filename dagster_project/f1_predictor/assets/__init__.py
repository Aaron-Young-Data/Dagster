from dagster import load_assets_from_package_module, load_assets_from_modules
from .f1_predictor import predictor_assets
from .predictor_evaluation import evaluation_assets
from .dnn_f1_predictor import dnn_model_assets

F1_PREDICTOR = "ML_project"
f1_predictor_assets = load_assets_from_package_module(package_module=f1_predictor, group_name=F1_PREDICTOR)

F1_PREDICTOR_EVAL = 'predictor_evaluation'
f1_predictor_evaluation_assets = load_assets_from_package_module(package_module=predictor_evaluation,
                                                                 group_name=F1_PREDICTOR_EVAL)

F1_DNN_MODEL_CREATE = 'DNN_model_creation'
f1_dnn_model_creation_assets = load_assets_from_package_module(package_module=dnn_f1_predictor,
                                                               group_name=F1_DNN_MODEL_CREATE)
