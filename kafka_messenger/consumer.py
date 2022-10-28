import json
import logging
import sys

from kafka import KafkaConsumer

from configs.config import BOOTSTRAP_SERVER

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MyConsumer:
    # link to documentation to KafkaConsumer: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    consumer = KafkaConsumer(
        'OpenSky_data_ingestion',  # topic name
        bootstrap_servers=BOOTSTRAP_SERVER,  # default port is 9092
        auto_offset_reset='earliest'  # a policy for resetting offsets on OffsetOutOfRange errors, default is 'latest'
    )

    def run_consumer(self):
        # state is a single chunk of data in topic
        for state in MyConsumer.consumer:
            # state is an object of a ConsumerRecord class (kafka.consumer.fetcher.ConsumerRecord),
            # I want to get information about state, so using state.value:
            logging.info(json.loads(state.value))
