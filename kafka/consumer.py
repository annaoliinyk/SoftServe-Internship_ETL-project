import json

from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'OpenSky_data_ingestion',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    for state in consumer:
        print(json.loads(state.value))
