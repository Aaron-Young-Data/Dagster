from dagster import (
    AssetSelection,
    define_asset_job)

from .assets import *
from .assets.database import database_restore
from .partitions import daily_partitions
from .assets.database.database_backup import *
from .assets.database.database_restore import *

backup_dir = os.getenv('BACKUP_DIR')

mysql_daily_backup_job = define_asset_job('mysql_daily_backup_job',
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

mysql_restore_job = define_asset_job('mysql_restore_job',
                                     selection=AssetSelection.assets(database_backup_restore),
                                     description='Job to restore the MySQL database from the most recent backup',
                                     config={'ops':
                                                 {'database_backup_restore':
                                                      {"config":
                                                           {'backup_file': ''}}}}
                                     )
