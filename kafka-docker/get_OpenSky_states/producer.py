import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from get_states_OpenSky import get_a_state


# Messages will be serialized as json
def serializer(state):
    return json.dumps(state).encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)


def generate_write_states():
    while True:
        state = get_a_state()
        print(f'Getting a state @{datetime.now()} | State info = {str(get_a_state())}')
        producer.send('OpenSky_data_ingestion', state)
        sleep_time = random.randint(1, 11)
        time.sleep(sleep_time)
