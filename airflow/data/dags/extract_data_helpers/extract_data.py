import logging
import sys

import requests

ALL_STATES_LINK = 'https://opensky-network.org/api/states/all'
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def get_states():
    states = []
    try:
        # try getting states as non-authenticated user:
        states = requests.get(ALL_STATES_LINK).json()
        logging.info(f"Authenticated to {ALL_STATES_LINK} and got all data for states")
    except:
        pass
    print(states)
    return states
