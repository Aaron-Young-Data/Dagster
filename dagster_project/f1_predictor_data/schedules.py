from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job
from .jobs import *
from .partitions import weekly_partitions


@schedule(job=update_calender_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_calender_job_weekly_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


@schedule(job=location_data_load_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_location_job_weekly_schedule(context: ScheduleEvaluationContext):
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


@schedule(job=weather_type_load_job,
          cron_schedule="0 12 * * 4",
          execution_timezone="GMT")
def update_weather_data_type_schedule(context: ScheduleEvaluationContext):
    return RunRequest()


weather_forecast_schedule = build_schedule_from_partitioned_job(
    job=weather_forecast_data_load_job,
    hour_of_day=0,
    minute_of_hour=5
)
