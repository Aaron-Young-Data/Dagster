from dagster import asset, Output, MetadataValue
import fastf1
from datetime import date
import pandas as pd
import os

data_loc = os.getenv('DATA_STORE_LOC')

@asset
def get_calender_data(context):
    today = date.today()
    year = today.year
    calendar = fastf1.get_event_schedule(year=year, include_testing=False)
    return Output(
        value=calendar,
        metadata={
            'Markdown': MetadataValue.md(calendar.head().to_markdown()),
        }
    )


@asset
def calender_to_csv(context, get_calender_data: pd.DataFrame):
    df = get_calender_data
    df.to_csv(f"{data_loc}calender.csv")
    return
