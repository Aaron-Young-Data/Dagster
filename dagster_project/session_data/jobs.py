from dagster import (
    AssetSelection,
    define_asset_job,
    RetryPolicy,)

# Session Assets
from .assets.full_session.practice import *
from .assets.full_session.qualifying import *
from .assets.full_session.race import *
from .assets.pre_assets.pre_assets import *
from .assets.session.practice import *
from .assets.session.qualifying import *
from .assets.session.race import *
from datetime import datetime

first_year = 2018
last_year = datetime.today().year
year_list = [i for i in range(first_year, last_year + 1, 1)]

# Full Session Data Jobs
full_session_data_load_job = define_asset_job('full_session_data_load_job',
                                              selection=AssetSelection.assets(get_events_sql,
                                                                              get_full_race_data_api,
                                                                              clean_full_race_data,
                                                                              full_race_data_to_sql,
                                                                              get_full_quali_data_api,
                                                                              clean_full_quali_data,
                                                                              full_quali_data_to_sql,
                                                                              get_full_practice_data_api,
                                                                              clean_full_practice_data,
                                                                              full_practice_data_to_sql,
                                                                              get_full_race_lap_data_api,
                                                                              clean_full_race_lap_data,
                                                                              full_race_lap_data_to_sql,
                                                                              get_drivers_sql,
                                                                              get_teams_sql),
                                              description="Job to load all session data for a list of years "
                                                          "(2018+) and upload the data to MySQL",
                                              op_retry_policy=RetryPolicy(max_retries=3),
                                              config={'ops':
                                                          {'get_events_sql':
                                                               {"config":
                                                                    {'year_list': year_list
                                                                     }}}}
                                              )

full_race_data_load_job = define_asset_job('full_race_data_load_job',
                                           selection=AssetSelection.assets(get_events_sql,
                                                                           get_full_race_data_api,
                                                                           clean_full_race_data,
                                                                           full_race_data_to_sql),
                                           description="Job to load all race session data for a list of years "
                                                       "(2018+) and upload the data to MySQL",
                                           op_retry_policy=RetryPolicy(max_retries=3),
                                           config={'ops':
                                                       {'get_events_sql':
                                                            {"config":
                                                                 {'year_list': year_list
                                                                  }}}}
                                           )

full_qualifying_data_load_job = define_asset_job('full_qualifying_data_load_job',
                                                 selection=AssetSelection.assets(get_events_sql,
                                                                                 get_full_quali_data_api,
                                                                                 clean_full_quali_data,
                                                                                 full_quali_data_to_sql),
                                                 description="Job to load all quali session data for a list of years "
                                                             "(2018+) and upload the data to MySQL",
                                                 op_retry_policy=RetryPolicy(max_retries=3),
                                                 config={'ops':
                                                             {'get_events_sql':
                                                                  {"config":
                                                                       {'year_list': year_list
                                                                        }}}}
                                                 )

full_practice_data_load_job = define_asset_job('full_practice_data_load_job',
                                               selection=AssetSelection.assets(get_events_sql,
                                                                               get_full_practice_data_api,
                                                                               clean_full_practice_data,
                                                                               full_practice_data_to_sql),
                                               description="Job to load all practice session data for a list of years "
                                                           "(2018+) and upload the data to MySQL",
                                               op_retry_policy=RetryPolicy(max_retries=3),
                                               config={'ops':
                                                           {'get_events_sql':
                                                                {"config":
                                                                     {'year_list': year_list
                                                                      }}}}
                                               )

full_race_laps_data_load_job = define_asset_job('full_race_laps_data_load_job',
                                                selection=AssetSelection.assets(get_events_sql,
                                                                                get_full_race_lap_data_api,
                                                                                clean_full_race_lap_data,
                                                                                full_race_lap_data_to_sql),
                                                description="Job to load the Race Laps for the config provided.",
                                                op_retry_policy=RetryPolicy(max_retries=3),
                                                config={'ops':
                                                            {'get_events_sql':
                                                                 {"config":
                                                                      {'year_list': year_list
                                                                       }}}}
                                                )

# Single Session Load Jobs
practice_data_load_job = define_asset_job('practice_data_load_job',
                                          selection=AssetSelection.assets(get_drivers_sql,
                                                                          get_teams_sql,
                                                                          get_practice_data_api,
                                                                          clean_practice_data,
                                                                          practice_data_to_sql),
                                          description="Job to load the practice session for the config provided.",
                                          config={'ops':
                                                      {'get_practice_data_api':
                                                           {"config":
                                                                {'practice_num': 1,
                                                                 'round_number': 1,
                                                                 'year': 2025
                                                                 }}}}
                                          )

quali_data_load_job = define_asset_job('qualifying_data_load_job',
                                       selection=AssetSelection.assets(get_drivers_sql,
                                                                       get_teams_sql,
                                                                       get_quali_data_api,
                                                                       clean_quali_data,
                                                                       quali_data_to_sql),
                                       description="Job to load the Qualifying session for the config provided.",
                                       config={'ops':
                                                   {'get_quali_data_api':
                                                        {"config":
                                                             {'sprint': False,
                                                              'round_number': 1,
                                                              'year': 2025
                                                              }}}}
                                       )

race_data_load_job = define_asset_job('race_data_load_job',
                                      selection=AssetSelection.assets(get_race_data_api,
                                                                      clean_race_data,
                                                                      race_data_to_sql),
                                      description="Job to load the Race session for the config provided.",
                                      config={'ops':
                                                  {'get_race_data_api':
                                                       {"config":
                                                            {'sprint': False,
                                                             'round_number': 1,
                                                             'year': 2025
                                                             }}}}
                                      )

race_laps_data_load_job = define_asset_job('race_laps_data_load_job',
                                           selection=AssetSelection.assets(get_drivers_sql,
                                                                           get_teams_sql,
                                                                           get_race_lap_data_api,
                                                                           clean_race_lap_data,
                                                                           race_lap_data_to_sql),
                                           description="Job to load the Race Laps for the config provided.",
                                           config={'ops':
                                                       {'get_race_lap_data_api':
                                                            {"config":
                                                                 {'sprint': False,
                                                                  'round_number': 1,
                                                                  'year': 2025
                                                                  }}}}
                                           )
