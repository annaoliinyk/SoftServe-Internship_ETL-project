from kafka_messenger.data_editor import StatesExtractor
from test_data_extraction.test_config import STATES_FOR_TEST, STATES_ONLY


def test_states_extraction():
    assert StatesExtractor().extract_states_from_dict(STATES_FOR_TEST) == STATES_ONLY


def test_state_info_dict():
    state = STATES_ONLY[0]
    expected_result_dict = {'icao24': '4b1814', 'values': ['4b1814', 'SWR60C  ', 'Switzerland']}
    assert StatesExtractor().return_state_info_dict(state) == expected_result_dict
