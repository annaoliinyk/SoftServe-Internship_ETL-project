from configs.config import STATE_DICT_KEYS


class StatesExtractor:
    # Data ingestion returns a dictionary with keys time and states, but I need only 'states' values
    def extract_states_from_dict(self, states_dict):
        states_only_list = states_dict['states']  # want to get states only in a form of list
        return states_only_list

    # provided a state info as a list, would like to get a dict with keys id (icao24) and other info
    def return_state_info_dict(self, state):
        icao24 = state[0]  # unique ICAO 24-bit address of the transponder in hex string representation, like id
        return {
            STATE_DICT_KEYS[0]: icao24,  # id
            STATE_DICT_KEYS[1]: state  # full info about the state
        }
