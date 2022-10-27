import logging
import sys
from datetime import datetime
import kafka.errors
from OpenSkyDataExtractor.get_states import DataIngestion
from kafka_messenger import producer, consumer

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    start_timestamp = datetime.now().timestamp()
    logging.info("Program started: {}".format(start_timestamp))
    data_ingestion_obj = DataIngestion()
    data_ingestion_obj.print_states()
    end_timestamp = datetime.now().timestamp()
    logging.info("Program ended: {}".format(end_timestamp))
    logging.info("Program running took: {}".format(end_timestamp - start_timestamp))
    try:
        consumer.MyConsumer()
        producer.MyProducer().send_all_states(data_ingestion_obj)
    except kafka.errors.NoBrokersAvailable:
        logging.error("Please execute container before running container and producer")


if __name__ == "__main__":
    main()
