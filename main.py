import logging
import sys
from datetime import datetime

from OpenSkyDataExtractor.get_states import DataIngestion

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    start_timestamp = datetime.now().timestamp()
    logging.info("Program started: {}".format(start_timestamp))
    DataIngestion.print_states()
    end_timestamp = datetime.now().timestamp()
    logging.info("Program ended: {}".format(end_timestamp))
    logging.info("Program running took: {}".format(end_timestamp - start_timestamp))


if __name__ == "__main__":
    main()
