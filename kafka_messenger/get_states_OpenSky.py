from random import randint

from OpenSkyDataExtractor.get_states import DataIngestion


def get_a_state() -> dict:
    data_ingestion_obj = DataIngestion()
    # returns a dict with keys 'states' and 'time', many states and one time value:
    states_dict = data_ingestion_obj.get_states()
    states_only_list = states_dict['states']  # want to get states only in a form of list
    random_int = randint(0, len(states_only_list) - 1)
    return_state = states_only_list[random_int]
    icao24 = return_state[0]  # unique ICAO 24-bit address of the transponder in hex string representation, like an id
    return {
        'icao24': icao24,  # id
        'values': return_state  # full info about the state
    }


if __name__ == "__main__":
    print(get_a_state())
