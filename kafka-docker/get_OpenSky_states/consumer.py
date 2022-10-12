import json

from kafka import KafkaConsumer


class ConsumerOpenSkyStates(KafkaConsumer):
    consumer = KafkaConsumer(
        'OpenSky_data_ingestion',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )

    def read_state(self):
        for state in self.consumer:
            print(json.loads(state.value))
