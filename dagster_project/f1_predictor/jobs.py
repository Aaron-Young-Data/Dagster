from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .partitions import daily_partitions
from .assets.pre_assets.pre_assets import *
from .assets.predictor.qualifying import *
from .assets.evaluation.qualifying import *

create_qualifying_prediction_job = define_asset_job("qualifying_prediction_job",
                                                    selection=AssetSelection.assets(session_info,
                                                                                    sql_driver_data,
                                                                                    qualifying_session_data_from_sql,
                                                                                    qualifying_training_data_from_sql,
                                                                                    create_qualifying_prediction_model,
                                                                                    create_qualifying_prediction,
                                                                                    qualifying_prediction_data_to_sql,
                                                                                    create_qualifying_prediction_img,
                                                                                    send_qualifying_prediction_discord),
                                                    description="Job to predict the f1 qualifying and output to discord",
                                                    config={'ops':
                                                                {'session_info':
                                                                     {"config":
                                                                          {'round_number': 1,
                                                                           'year': 2025
                                                                           }}}})

evaluate_qualifying_prediction_job = define_asset_job('evaluate_qualifying_prediction_job',
                                                      selection=AssetSelection.assets(session_info,
                                                                                      get_qualifying_evaluation_data,
                                                                                      create_qualifying_position_evaluation_img,
                                                                                      create_qualifying_laptime_evaluation_img,
                                                                                      send_qualifying_evaluation_discord),
                                                      description='Job to evaluate the performance of the qualifying '
                                                                  'prediction that was made and output to discord',
                                                      config={'ops':
                                                                {'session_info':
                                                                     {"config":
                                                                          {'round_number': 1,
                                                                           'year': 2025
                                                                           }}}})
