from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job
from .jobs import *
from .partitions import daily_partitions


@schedule(job=mysql_daily_backup,
          cron_schedule="0 0 * * *",
          execution_timezone="GMT")
def mysql_daily_backup_schedule(context: ScheduleEvaluationContext):
    return RunRequest()
