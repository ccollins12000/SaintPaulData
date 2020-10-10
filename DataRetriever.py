import requests
import json
import pandas as pd
import math
import time

class DataRetriever:
    def __init__(self, api_url, app_token):
        """The constructor for the data retriever

        args:
            api_url (str): The url/end point to the data api
            app_token (str): The api token Sign up for token here https://dev.socrata.com/register
        """
        self._APP_TOKEN = app_token
        self._API_URL = api_url
        self._HEADERS = {'Accept':'application/json'}
        self._PARAMETERS = {"$$app_token": app_token, '$select':'count(*)'}
        response = requests.get(url=api_url, headers=self._HEADERS, params=self._PARAMETERS)
        self._RECORD_COUNT = int(next(iter(json.loads(response.text)[0].items()))[1])
        self._PARAMETERS.pop('$select')

    def fetch_data(self):
        """Call this to fetch all the data from the api

        returns:
            A list of all the reults
        """
        data = []

        parameters = self._PARAMETERS
        parameters.update({'$limit':1000})
        pages = math.ceil(self._RECORD_COUNT / 1000)

        for page in range(0, pages):
            print(page, ' of ', pages)
            parameters.update({'$offset':page})
            response = requests.get(url=self._API_URL, headers=self._HEADERS, params=self._PARAMETERS)
            data.extend(json.loads(response.text))
            time.sleep(.2)

        return data


def update_data_sets(api_token):
    """Downloads all the data to CSV files

    args:
        api_token (str):  The api token Sign up for token here https://dev.socrata.com/register
    """
    urls = {
        'bike_ped_accidents': 'https://information.stpaul.gov/resource/bw92-5h94.json',
        'crime': 'https://information.stpaul.gov/resource/gppb-g9cg.json',
        'budget': 'https://information.stpaul.gov/resource/hgx3-a7ev.json',
        'police_grid_shapes': 'https://information.stpaul.gov/resource/xfxz-iqwn.json',
        'district_council_shapes': 'https://information.stpaul.gov/resource/kiaj-ufhe.json',
        'council_ward_shapes': 'https://information.stpaul.gov/resource/tc7y-gce5.json'
    }

    for (data_set, url) in urls.items():
        retriever = DataRetriever(url, app_token)
        data = retriever.fetch_data()
        pd.DataFrame(data).to_csv(r'data/' + data_set + '.csv')


if __name__ == "__main__":
    app_token = input('Enter your app token: ')
    update_data_sets(app_token)
