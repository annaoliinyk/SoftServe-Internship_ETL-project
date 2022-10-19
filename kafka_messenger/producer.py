import json
import logging
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

from kafka_messenger.get_states_OpenSky import RandomState

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MyProducer:
    def __init__(self):
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=self.serializer
        )

    # Messages will be serialized as json
    def serializer(self, state):
        return json.dumps(state).encode('utf-8')

    def run_producer(self, data_ingestion_obj):
        while True:
            state = RandomState().get_random_state(data_ingestion_obj)
            logging.info(f'Getting a state @{datetime.now()} | State info = {str(state)}')
            self.producer.send('OpenSky_data_ingestion', state)
            sleep_time = random.randint(1, 11)
            time.sleep(sleep_time)
