from dagster import schedule, ScheduleEvaluationContext, RunRequest, build_schedule_from_partitioned_job
from .jobs import *
from .partitions import weekly_partitions


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
