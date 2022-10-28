import json
import logging
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

from configs.config import BOOTSTRAP_SERVER
from kafka_messenger.data_editor import StatesExtractor

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MyProducer:
    def __init__(self):
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVER],
            value_serializer=self.serializer
        )

    # Messages will be serialized as json
    def serializer(self, state):
        return json.dumps(state).encode('utf-8')

    def send_all_states(self, states_dict):
        states = StatesExtractor().extract_states_from_dict(states_dict)
        for state in states:
            state_as_dict = StatesExtractor().return_state_info_dict(state)
            logging.info(f'Getting all the states @{datetime.now()} | State info = {state_as_dict}')
            self.producer.send('OpenSky_data_ingestion', state)
            time.sleep(10)
