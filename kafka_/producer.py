import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from kafka_.get_states_OpenSky import get_a_state


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

    def run_producer(self):
        while True:
            state = get_a_state()
            print(f'Getting a state @{datetime.now()} | State info = {str(get_a_state())}')
            self.producer.send('OpenSky_data_ingestion', state)
            sleep_time = random.randint(1, 11)
            time.sleep(sleep_time)
