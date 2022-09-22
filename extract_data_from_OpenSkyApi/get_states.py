import json
import logging
import pprint
import sys

import requests

from configs.get_api_user_credentials import get_credentials_from_file

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class DataIngestion:
    def __init__(self):
        self.states = {}

    def get_states(self):
        credentials = get_credentials_from_file()
        logging.info("Authenticating to https://opensky-network.org/api/states/all and getting all data for states")
        self.states = requests.get("https://opensky-network.org/api/states/all", data=credentials).json()
        DataIngestion.save_requests_to_json(self)
        return self.states

    def save_requests_to_json(self, filename="all_states.json"):
        with open(filename, 'w') as fp:
            json.dump(self.states, fp)

    @classmethod
    def print_states(cls):
        pprint.pprint(DataIngestion().get_states())
        return
