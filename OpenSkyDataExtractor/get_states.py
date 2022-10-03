import json
import logging
import pprint
import sys

import requests
import configs

from configs.get_api_user_credentials import get_credentials_from_file

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class DataIngestion:
    def __init__(self):
        try:
            self.states = self.get_states()
        except Exception as e:
            logging.info("Exception occurred: ", e)

    def get_states(self):
        credentials = get_credentials_from_file()
        logging.info("Authenticating to https://opensky-network.org/api/states/all and getting all data for states")
        states = requests.get("https://opensky-network.org/api/states/all", data=credentials).json()
        return states

    def save_requests_to_json(self, filename="all_states.json"):
        with open(filename, 'w') as fp:
            json.dump(self.states, fp)

    def print_states(self):
        pprint.pprint(self.states)
        return
