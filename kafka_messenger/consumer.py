import logging
import sys

from kafka import KafkaConsumer

from configs.config import BOOTSTRAP_SERVER

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MyConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'OpenSky_data_ingestion',
            bootstrap_servers=BOOTSTRAP_SERVER,
            auto_offset_reset='earliest'
        )
