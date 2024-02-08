from dagster import asset, Output, MetadataValue
import fastf1
from datetime import date
import pandas as pd


@asset
def get_calender(context):
    today = date.today()
    year = today.year
    calendar = fastf1.get_event_schedule(year=year, include_testing=False)
    return Output(
        value=calendar,
        metadata={
            'Markdown': calendar.head().to_markdown(),
        }
    )


@asset
def calender_to_csv(context, get_calender: pd.DataFrame):
    df = get_calender
    df.to_csv("dagster_project/data_storage/calender.csv")
    return
