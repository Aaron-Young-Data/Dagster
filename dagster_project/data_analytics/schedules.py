from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job
from .jobs import *
from .partitions import daily_partitions


@schedule(job=data_load_track_status_data_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_track_status_data_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()

