from random import randint


class RandomStateExtractor:
    def extract_random_state(self, states_dict):
        states_only_list = states_dict['states']  # want to get states only in a form of list
        maximal_index = len(states_only_list) - 1
        random_int = randint(0, maximal_index)
        random_state = states_only_list[random_int]
        return random_state
