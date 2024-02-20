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
    year_list = list(range(2018, year+1))
    context.log.info(year_list)
    full_calender = pd.DataFrame()
    for year in year_list:
        calendar = fastf1.get_event_schedule(year=year, include_testing=False)
        full_calender = pd.concat([full_calender, calendar])
    return Output(
        value=full_calender,
        metadata={
            'Markdown': MetadataValue.md(full_calender.head().to_markdown()),
            'Rows': len(full_calender),
        }
    )


@asset
def calender_to_csv(context, get_calender_data: pd.DataFrame):
    df = get_calender_data
    df.to_csv(f"{data_loc}calender.csv")
    return


@asset(io_manager_key='sql_io_manager_dev', key_prefix=['ml_project_dev', 'f1_calender', 'cleanup'])
def calender_to_sql(context, get_calender_data: pd.DataFrame):
    df = get_calender_data.drop(['Country',
                                 'Location',
                                 'OfficialEventName',
                                 'F1ApiSupport',
                                 'Session1Date',
                                 'Session2Date',
                                 'Session3Date',
                                 'Session4Date',
                                 'Session5Date'], axis=1)
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )
