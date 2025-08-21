from datetime import datetime, timedelta
from dagster import WeeklyPartitionsDefinition, DailyPartitionsDefinition

today = datetime.today()

weekly_partitions = WeeklyPartitionsDefinition(start_date=today - timedelta(weeks=8), end_offset=1, day_offset=4)

daily_partitions = DailyPartitionsDefinition(start_date=today - timedelta(weeks=2, days=1))