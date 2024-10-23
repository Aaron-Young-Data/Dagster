from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.dnn_f1_predictor.dnn_predictor_assets import *
from .assets.dnn_f1_predictor.dnn_model_assets import *
from .partitions import daily_partitions

create_prediction_job = define_asset_job("F1_prediction_job",
                                         selection=AssetSelection.groups(F1_PREDICTOR),
                                         description="Job to predict the f1 qualifying and output to discord",
                                         config={'ops':
                                                     {'session_info':
                                                          {"config":
                                                               {'event_type': 'conventional',
                                                                'event_name': 'Abu Dhabi Grand Prix',
                                                                'year': 2023
                                                                }}}})

evaluate_prediction_job = define_asset_job('evaluation_prediction_job',
                                           selection=AssetSelection.groups(F1_PREDICTOR_EVAL),
                                           description='Job to evaluate the performance of the prediction that was made'
                                                       ' and output to discord',
                                           config={'ops':
                                                       {'quali_session_info':
                                                            {"config":
                                                                 {'event_name': 'Abu Dhabi Grand Prix',
                                                                  'year': 2023
                                                                  }}}})

create_dnn_model_job = define_asset_job('create_dnn_model_job',
                                        selection=AssetSelection.assets(dnn_training_data_from_sql,
                                                                        test_train_split,
                                                                        build_dnn_model,
                                                                        train_dnn_model,
                                                                        evaluate_dnn_model,
                                                                        save_dnn_model,
                                                                        evaluate_dnn_model_to_discord),
                                        description='Job to create an updated DNN model when new data is available and'
                                                    'outputs new model evaluation to discord')

create_prediction_dnn_model_job = define_asset_job('create_prediction_dnn_model_job',
                                                   selection=AssetSelection.assets(load_dnn_model,
                                                                                   dnn_model_session_info,
                                                                                   dnn_model_session_data_sql),
                                                   description='Job to create an updated DNN model when new data is'
                                                               'available and outputs new model evaluation to discord',
                                                   config={'ops':
                                                               {'dnn_model_session_info':
                                                                    {"config":
                                                                         {'event_type': 'conventional',
                                                                          'event_name': 'Abu Dhabi Grand Prix',
                                                                          'year': 2023
                                                                          }}}}
                                                   )
