import fastf1
from fastf1.core import Laps
import pandas as pd
import os

cache = os.getenv('FAST_F1_CACHE_LOC')

class GetData:
    def __init__(self):
        fastf1.Cache.enable_cache(cache)

    def get_calender(self, year: int, testing=False):
        # returns the information for the schedule (Location, Date, Format, Session(s))
        schedule = fastf1.get_event_schedule(year=year, include_testing=testing)
        if schedule.empty:
            raise Exception('No Data Collected!')
        else:
            return schedule[['EventName',
                             'EventDate',
                             'EventFormat',
                             'Session1',
                             'Session2',
                             'Session3',
                             'Session4',
                             'Session5']]

    def session_data(self, year: int, location: str, session=None):
        session_data = fastf1.get_session(year=year, gp=location, identifier=session)
        return session_data

    def session_list(self, session_df: pd.DataFrame):
        sessions = [x for c in session_df.columns for x in session_df[c].value_counts()[:1].index]
        return sessions

    def fastest_laps(self, session_data, weather_data=True):
        # returns the fastest laps from given session object
        # checks if the session_data is a session object
        try:
            session_data.load(weather=weather_data)
        except AttributeError:
            raise Exception('session_data is not a session object')
        except KeyError:
            return pd.DataFrame()
        try:
            drivers = pd.unique(session_data.laps['Driver'])
        except fastf1.core.DataNotLoadedError:
            return pd.DataFrame()

        list_fastest_laps = list()
        for drv in drivers:
            drvs_fastest_lap = session_data.laps.pick_driver(drv).pick_fastest()
            list_fastest_laps.append(drvs_fastest_lap)

        if weather_data:
            fastest_laps = Laps(list_fastest_laps).sort_values(by='Time').reset_index(drop=True).dropna(how='all')
            weather_df = session_data.weather_data
            fastest_laps = pd.merge_asof(fastest_laps, weather_df, on='Time', direction='nearest')
            fastest_laps.sort_values(by='LapTime', inplace=True)
        else:
            fastest_laps = Laps(list_fastest_laps).sort_values(by='LapTime').reset_index(drop=True).dropna(how='all')
        return fastest_laps

    def all_laps(self, session_data, weather_data=True):

        try:
            session_data.load(weather=weather_data)
        except AttributeError:
            raise Exception('session_data is not a session object')
        except KeyError:
            return pd.DataFrame()
        try:
            drivers = pd.unique(session_data.laps['Driver'])
        except fastf1.core.DataNotLoadedError:
            return pd.DataFrame()

        df = pd.DataFrame()

        for drv in session_data.drivers:
            drv_laps = session_data.laps.pick_driver(drv).sort_values(by='Time')
            if weather_data:
                weather_df = session_data.weather_data
                drv_laps = pd.merge_asof(weather_df, drv_laps, on='Time', direction='nearest')
            df = pd.concat([df, drv_laps], ignore_index=True)

        return df

class CleanData:
    def __init__(self):
        fastf1.Cache.enable_cache(cache)

    def order_laps_delta(self, laps: pd.DataFrame, include_pos=True):
        # Order dataframe by laptime column
        pole_lap = laps.pick_fastest()
        laps['LapTimeDelta'] = laps['LapTime'] - pole_lap['LapTime']
        if include_pos:
            laps['Final_POS'] = laps['LapTime'].rank(method='first')
        return laps

    def time_cols_to_seconds(self, column_names: list, dataframe: pd.DataFrame, new_column=False):
        # This will convert the given date/time columns into the second equivalent
        for col in column_names:
            try:
                dataframe[col]
                if new_column:
                    dataframe[f'{col}_seconds'] = dataframe[col].dt.total_seconds()
                else:
                    dataframe[col] = dataframe[col].dt.total_seconds()
            except KeyError:
                raise Exception(f'Column {col} is not in the dataframe!')
            except AttributeError:
                raise Exception(f'Column {col} is not datetime value')

        return dataframe
