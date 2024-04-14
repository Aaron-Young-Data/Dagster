from datetime import datetime, timedelta
from dagster import DailyPartitionsDefinition

today = datetime.today()
partition_start_date = today - timedelta(days=14)

daily_partitions = DailyPartitionsDefinition(start_date=partition_start_date, end_offset=1, timezone="GMT")
