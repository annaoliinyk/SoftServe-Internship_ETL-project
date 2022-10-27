import logging
import sys

from kafka import KafkaConsumer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MyConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'OpenSky_data_ingestion',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest'
        )
