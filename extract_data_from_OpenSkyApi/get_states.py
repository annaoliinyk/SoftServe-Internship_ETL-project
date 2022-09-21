import json
import logging
import sys

import requests

# want to see if connection is successful:
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def get_credentials_from_file():
    file = open(r"configs\opensky_credentials", "r").read()
    # get username, password from first and second lines of text file:
    username, password = file.split("\n")[0], file.split("\n")[1]
    return {"login": username, "password": password}


def get_states():
    credentials = get_credentials_from_file()
    states = requests.get("https://opensky-network.org/api/states/all", data=credentials).json()
    # save_requests_to_json("all_states.json", states)
    return states


def save_requests_to_json(filename, my_dict):
    with open(filename, 'w') as fp:
        json.dump(my_dict, fp)
