import logging
import sys
from datetime import datetime

import kafka.errors
from kafka_messenger import producer, consumer
from OpenSkyDataExtractor.get_states import DataIngestion
from pyspark_analysis.pyspark_tasks import MySparkCalculations

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    start_timestamp = datetime.now().timestamp()
    logging.info("Program started: {}".format(start_timestamp))
    # Extract - Ingest data from OpenSky Api
    data_ingestion_obj = DataIngestion()
    data_ingestion_obj.print_states()  # just prints states without writing their values
    all_states = data_ingestion_obj.get_states()  # get all states as a dictionary and assign to variable
    # try to connect kafka and send single state as a message
    try:
        producer.MyProducer().send_all_states(all_states)
        consumer.MyConsumer().run_consumer()
    # this error occurs when Docker containers are not running
    except kafka.errors.NoBrokersAvailable:
        logging.error("Please execute container before running container and producer")
    # spark:
    spark_task_obj = MySparkCalculations()
    spark_task_obj.get_highest_altitude()
    spark_task_obj.get_highest_velocity()
    spark_task_obj.get_airplanes_count_by_airport()
    spark_task_obj.stop_spark()
    # measure time it took to run the program
    end_timestamp = datetime.now().timestamp()
    logging.info("Program ended: {}".format(end_timestamp))
    logging.info("Program running took: {}".format(end_timestamp - start_timestamp))


if __name__ == "__main__":
    main()
