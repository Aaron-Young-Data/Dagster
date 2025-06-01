from dagster import ConfigurableResource
import requests
import pandas as pd


class JolpiResource(ConfigurableResource):
    @property
    def query_url(self) -> str:
        return 'https://api.jolpi.ca/ergast/f1/'

    def get_tracks(self, year: int):
        query = self.query_url + f'{year}/circuits/'
        response = requests.get(query)
        if response.status_code == 200:
            circuit_data = response.json()['MRData']['CircuitTable']['Circuits']
            df = pd.DataFrame(circuit_data)
            loc_df = pd.json_normalize(df['Location'])
            final_df = pd.concat([df, loc_df], axis=1).drop(columns='Location')

            final_df.rename(columns={'circuitId': 'TRACK_ID',
                                     'url': 'URL',
                                     'circuitName': 'TRACK_NAME',
                                     'lat': 'LATITUDE',
                                     'long': 'LONGITUDE',
                                     'locality': 'LOCATION',
                                     'country': 'COUNTRY'
                                     },
                            inplace=True)
        else:
            raise Exception(f'Error: {response.status_code}')

        return final_df

    def get_track_event(self, year: int):
        query = self.query_url + f'{year}/races/'
        response = requests.get(query)
        if response.status_code == 200:
            circuit_event_data = response.json()['MRData']['RaceTable']['Races']
            df = pd.DataFrame(circuit_event_data)
            loc_df = pd.json_normalize(df['Circuit'])
            final_df = pd.concat([df, loc_df], axis=1).drop(columns='Circuit')[['season', 'round', 'circuitId']]
            final_df.loc[:, 'EVENT_CD'] = final_df['season'] + final_df['round']
            final_df.drop(columns=['season', 'round'], inplace=True)
            final_df.rename(columns={'circuitId': 'TRACK_ID'}, inplace=True)
        else:
            raise Exception(f'Error: {response.status_code}')

        return final_df

    def get_drivers(self, year: int):
        query = self.query_url + f'{year}/drivers/'
        response = requests.get(query)
        if response.status_code == 200:
            driver_data = response.json()['MRData']['DriverTable']['Drivers']
            df = pd.DataFrame(driver_data)
        else:
            raise Exception(f'Error: {response.status_code}')

        return df

    def get_constructors(self, year: int):
        query = self.query_url + f'{year}/constructors/'
        response = requests.get(query)
        if response.status_code == 200:
            driver_data = response.json()['MRData']['ConstructorTable']['Constructors']
            df = pd.DataFrame(driver_data)
        else:
            raise Exception(f'Error: {response.status_code}')

        return df

