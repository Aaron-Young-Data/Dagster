import fastf1
from fastf1.core import Laps
import pandas as pd
import os
from typing import Literal, get_args

cache = os.getenv('FAST_F1_CACHE_LOC')

all_sessions = ['Practice 1',
                'Practice 2',
                'Practice 3',
                'Qualifying',
                'Sprint Qualifying',
                'Sprint Shootout',
                'Race',
                'Sprint']

practice_sessions = Literal['Practice 1', 'Practice 2', 'Practice 3']
quali_sessions = Literal['Qualifying', 'Sprint Qualifying', 'Sprint Shootout']
race_sessions = Literal['Race', 'Sprint']


def datetime_to_seconds(df, columns: list):
    for col in columns:
        try:
            df[col]
            df[col] = df[col].dt.total_seconds()
        except KeyError:
            raise Exception(f'Column {col} is not in the dataframe!')
        except AttributeError:
            raise Exception(f'Column {col} is not datetime value')

    return df


class FastF1Utils:
    def __init__(self, year: int, location: str, session: all_sessions):

        self.weather_df = None
        self.laptime_df = None
        self.results_df = None

        fastf1.Cache.enable_cache(cache)

        self.year = year
        self.location = location
        self.session = session

        self.session_data = fastf1.get_session(year=self.year,
                                               gp=self.location,
                                               identifier=self.session)

    def _merge_weather_df(self, df):
        modified_df = pd.merge_asof(df.sort_values(by='Time'), self.weather_df, on='Time', direction='nearest')
        modified_df.sort_values(by='Position', inplace=True)
        modified_df.drop(columns=['Time'], inplace=True)
        modified_df.reset_index(drop=True, inplace=True)

        return modified_df

    def _load_session_data(self):
        self.session_data.load(weather=True)
        self.results_df = self.session_data.results
        self.lap_time_df = self.session_data.laps
        self.weather_df = self.session_data.weather_data

    def _get_results_quali(self):
        # Get only the needed columns from the data dfs
        results = self.results_df[['DriverNumber', 'Abbreviation', 'TeamName', 'Q1', 'Q2', 'Q3', 'Position']]
        lap_times = self.lap_time_df[['Time', 'Driver', 'LapTime', 'Compound']].dropna(how='any')

        # Convert all columns that are a datetime to seconds
        results = datetime_to_seconds(results, ['Q1', 'Q2', 'Q3'])
        lap_times = datetime_to_seconds(lap_times, ['LapTime'])

        # Create Columns that is the final orders lap times
        results.loc[:, 'Q_time'] = results["Q3"].fillna(results["Q2"]).fillna(results["Q1"])

        # fill the null values for Q1 - Q3 with 0s
        results = results.fillna({'Q1': 0, 'Q2': 0, 'Q3': 0}).copy()

        # Merge on the lap time and driver to get the time the lap was set
        results_merged = pd.merge(results,
                                  lap_times,
                                  how='left',
                                  left_on=['Abbreviation', 'Q_time'],
                                  right_on=['Driver', 'LapTime']).drop(columns=['Driver', 'LapTime'])

        # Group to remove dupes where a drive has set the same time multiple time and get the max time for that lap time
        results_merged = results_merged.groupby(by=['DriverNumber',
                                                    'Abbreviation',
                                                    'TeamName',
                                                    'Q1',
                                                    'Q2',
                                                    'Q3',
                                                    'Position',
                                                    'Q_time']).max().reset_index().sort_values(by='Position').reset_index(drop=True)

        # Combine data with the weather data and set to self.final_results_df
        final_df = self._merge_weather_df(results_merged)

        self.final_results_df = final_df.drop(columns=['Humidity', 'Pressure'])

    def get_session_results(self):
        self._load_session_data()
        if self.session in get_args(quali_sessions):
            self._get_results_quali()
        elif self.session in get_args(race_sessions):
            pass
        elif self.session in get_args(practice_sessions):
            pass
        else:
            raise Exception(f'Session {self.session} not supported!')
        return self.final_results_df
