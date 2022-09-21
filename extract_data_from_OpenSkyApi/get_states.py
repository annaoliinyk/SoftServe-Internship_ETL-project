import json
import logging
import sys

import requests

# want to see if connection is successful:
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_credentials_from_file():
    with open(r"configs\opensky_credentials.json") as json_file:
        data = json.load(json_file)
        # get username, password from first and second lines of text file:
        username, password = data["username"], data["password"]
        return {"login": username, "password": password}


def get_states():
    credentials = get_credentials_from_file()
    states = requests.get("https://opensky-network.org/api/states/all", data=credentials).json()
    # save_requests_to_json("all_states.json", states)
    return states


def save_requests_to_json(filename, my_dict):
    with open(filename, 'w') as fp:
        json.dump(my_dict, fp)
