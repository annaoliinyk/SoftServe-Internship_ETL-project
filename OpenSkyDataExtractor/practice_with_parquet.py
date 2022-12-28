import logging
import sys

import pandas as pd

JSON_PATH = r"all_states.json"
PARQUET_PATH = r"all_states.parquet"

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class JsonToParquet:
    def __init__(self):
        self.json_contents_df = self.read_json_file()
        self.parquet_created: bool = self.dataframe_to_parquet()

    def read_json_file(self):
        try:
            df = pd.read_json(JSON_PATH)
            return df
        except Exception as e:
            logging.error("Can't read json file, see the error: " + str(e))
            quit()

    def dataframe_to_parquet(self):
        try:
            df_as_str = self.json_contents_df.astype(str)
            df_as_str.to_parquet(r"all_states.parquet")
            logging.info("Saved to parquet format successfully!")
            return True
        except Exception as e:
            logging.error("Something went wrong; " + str(e))

    def show_df_head(self):
        logging.info(self.json_contents_df.head(10))


class ReadParquetFile:
    def __init__(self):
        self.df = pd.read_parquet(PARQUET_PATH)

    def show_parquet_file_head(self):
        logging.info(self.df.head(10))


if __name__ == '__main__':
    JsonToParquet().show_df_head()
    ReadParquetFile().show_parquet_file_head()
