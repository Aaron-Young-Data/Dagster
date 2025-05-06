import subprocess
import datetime
import shutil
import os
from dagster import asset, Output, MetadataValue, multi_asset, AssetOut, AssetExecutionContext
import time

data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')


@asset(config_schema={'backup_file': str})
def database_backup_restore(context: AssetExecutionContext,
                            database_backup: str):
    if context.op_config['backup_file'] == '':
        backup_file = database_backup
    else:
        backup_file = context.op_config['backup_file']

    if not os.path.isfile(backup_file):
        raise Exception(f"Error: Backup file '{backup_file}' does not exist.")

    context.log.info(f'Restoring from {backup_file}')

    dump_command = f'mysql -h {server} -P {port} -u {user} --password={password} < "{backup_file}"'

    context.log.info('Restoring database from backup file')
    subprocess.run(dump_command, shell=True)
    context.log.info('Restoring database complete')

    if not os.path.isfile(backup_file):
        raise Exception(f"Error: Backup file '{backup_file}' has not been created.")

    return