from dagster import (
    AssetSelection,
    define_asset_job,
    ScheduleDefinition)

from .assets import *
from .assets.ML_project.data_update.session import *
from .assets.ML_project.data_update.calender import *

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

update_calender_job = define_asset_job("update_calender_job",
                                       selection=AssetSelection.assets(get_calender_data, calender_to_csv),
                                       description="Job to update the current years F1 calender")

session_data_load_job = define_asset_job("session_data_load_job",
                                         selection=AssetSelection.assets(get_session_data, session_to_file),
                                         description="Job to pull of session data for a list of years (2018+)",
                                         config={'ops':
                                                     {'get_session_data':
                                                          {"config":
                                                               {'year_list': [2018, 2019, 2020, 2021, 2022, 2023]
                                                                }}}})
