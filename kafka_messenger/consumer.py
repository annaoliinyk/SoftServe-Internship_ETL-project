import json

from kafka import KafkaConsumer


class MyConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'OpenSky_data_ingestion',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest'
        )

    def run_consumer(self):
        for state in self.consumer:
            print(json.loads(state.value))
