import json
import logging
import pprint
import sys

import requests

from configs.get_api_user_credentials import get_credentials_from_file

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class DataIngestion:
    def __init__(self):
        try:
            self.states = self.get_states()
        except Exception as e:
            logging.info("Exception occurred: ", e)

    def get_states(self):
        try:
            # try getting states as authenticated user:
            credentials = get_credentials_from_file()
            states = requests.get("https://opensky-network.org/api/states/all", data=credentials).json()
            logging.info("Logged in and authenticated to https://opensky-network.org/api/states/all and got all data "
                         "for states")
        except FileNotFoundError:
            # try getting states as non-authenticated user:
            states = requests.get("https://opensky-network.org/api/states/all").json()
            logging.info("Authenticated to https://opensky-network.org/api/states/all and got all data for states")
        except:
            # else get data from json file
            with open('all_states.json', 'r') as f:
                states = json.load(f)
            logging.info("Got all data for states from local file")
        return states

    def save_requests_to_json(self, filename="all_states.json"):
        with open(filename, 'w') as fp:
            json.dump(self.states, fp)

    def print_states(self):
        pprint.pprint(self.states)
        return
