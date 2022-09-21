import pprint

import extract_data_from_OpenSkyApi.get_states


def print_states():
    states = get_states.get_states()
    pprint.pprint(states)
