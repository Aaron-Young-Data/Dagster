from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job
from .jobs import *
from .partitions import weekly_partitions


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


@schedule(job=track_event_data_load_job,
          cron_schedule="10 12 * * 4",
          execution_timezone="GMT")
def update_track_event_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=update_driver_jobs,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_driver_data_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=update_constructors_jobs,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_constructor_data_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=update_dim_session_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_dim_session_data_schedule(context: ScheduleEvaluationContext):
    return RunRequest()
