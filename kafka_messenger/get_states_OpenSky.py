from kafka_messenger.data_editor import RandomStateExtractor

STATE_DICT_KEYS = ['icao24', 'values']


class RandomState:
    def get_random_state(self, data_ingestion_obj) -> dict:
        # returns a dict with keys 'states' and 'time', many states and one time value:
        states_dict = data_ingestion_obj.get_states()
        random_state = RandomStateExtractor().extract_random_state(states_dict)
        icao24 = random_state[0]  # unique ICAO 24-bit address of the transponder in hex string representation, like id
        return {
            STATE_DICT_KEYS[0]: icao24,  # id
            STATE_DICT_KEYS[1]: random_state  # full info about the state
        }
