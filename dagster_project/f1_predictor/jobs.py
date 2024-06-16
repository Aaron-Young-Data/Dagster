from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
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
                                        selection=AssetSelection.groups(F1_DNN_MODEL_CREATE),
                                        description='Job to create an updated DNN model when new data is available and'
                                                    'outputs new model evaluation to discord')
