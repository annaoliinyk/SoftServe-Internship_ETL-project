import json
import logging
import sys
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka_messenger.get_states_OpenSky import StatesOpenSky
from configs.config import BOOTSTRAP_SERVER

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

    def send_all_states(self, data_ingestion_obj):
        states = StatesOpenSky().get_all_states(data_ingestion_obj)
        for state in states:
            logging.info(f'Getting all the states @{datetime.now()} | State info = '
                         f'{StatesOpenSky().return_state_info_dict(str(state))}')
            self.producer.send('OpenSky_data_ingestion', state)
            time.sleep(10)
