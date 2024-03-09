from dagster import schedule, ScheduleEvaluationContext, RunRequest
from .jobs import *


@schedule(job=update_calender_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_calender_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=compound_data_load_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_compound_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=track_data_load_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_track_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


