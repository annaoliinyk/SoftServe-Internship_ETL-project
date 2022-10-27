from configs.config import STATE_DICT_KEYS
from kafka_messenger.data_editor import StatesExtractor


class StatesOpenSky:
    def get_all_states(self, data_ingestion_obj) -> dict:
        # returns a dict with keys 'states' and 'time', many states and one time value:
        states_dict = data_ingestion_obj.get_states()
        all_states = StatesExtractor().get_all_states(states_dict)
        return all_states

    def return_state_info_dict(self, state):
        icao24 = state[0]  # unique ICAO 24-bit address of the transponder in hex string representation, like id
        return {
            STATE_DICT_KEYS[0]: icao24,  # id
            STATE_DICT_KEYS[1]: state  # full info about the state
        }
