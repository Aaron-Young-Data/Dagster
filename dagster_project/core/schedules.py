from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job, ScheduleDefinition
from .jobs import *
from .partitions import daily_partitions

backup_dir = os.getenv('BACKUP_DIR')


mysql_daily_backup_schedule = ScheduleDefinition(name='mysql_daily_backup_schedule',
                                                 job=mysql_daily_backup_job,
                                                 cron_schedule='0 0 * * *',
                                                 execution_timezone='Europe/London',
                                                 run_config={'ops':
                                                             {'database_backup_cleanup':
                                                                  {"config":
                                                                       {'backup_directory': backup_dir,
                                                                        'archive_days': 14}},
                                                              'database_backup':
                                                                  {"config":
                                                                       {'type': 'auto'}
                                                                   }}}
                                                 )
