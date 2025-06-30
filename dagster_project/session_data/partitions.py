from datetime import datetime, timedelta
from dagster import WeeklyPartitionsDefinition, DailyPartitionsDefinition

today = datetime.today()
partition_start_date = today - timedelta(weeks=8)

weekly_partitions = WeeklyPartitionsDefinition(start_date=partition_start_date, end_offset=1, day_offset=4)