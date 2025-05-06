from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .partitions import daily_partitions
from .assets.database.database_backup import *

backup_dir = os.getenv('BACKUP_DIR')

mysql_daily_backup = define_asset_job('mysql_daily_backup',
                                      selection=AssetSelection.assets(database_backup_cleanup,
                                                                      database_backup),
                                      description='Job to backup the MySQL database daily',
                                      config={'ops':
                                                  {'database_backup_cleanup':
                                                       {"config":
                                                            {'backup_directory': backup_dir,
                                                             'archive_days': 14}},
                                                   'database_backup':
                                                       {"config":
                                                            {'type': 'manual'}
                                                        }}})
