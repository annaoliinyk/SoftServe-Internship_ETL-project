import logging
import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, min, sum
from pyspark.sql.types import *

from config import *

JSON_PATH = os.path.join(project_local_path, r"OpenSkyDataExtractor\all_states.json")
SPARK = SparkSession.builder.appName("SparkSQL").getOrCreate()
DF_SCHEMA = StructType([StructField("icao24", StringType(), True), StructField("callsign", StringType(), True),
                        StructField("origin_country", StringType(), True),
                        StructField("time_position", StringType(), True),
                        StructField("last_contact", StringType(), True), StructField("longitude", StringType(), True),
                        StructField("latitude", StringType(), True), StructField("baro_altitude", StringType(), True),
                        StructField("on_ground", StringType(), True), StructField("velocity", StringType(), True),
                        StructField("true_track", StringType(), True), StructField("vertical_rate", StringType(), True),
                        StructField("sensors", ArrayType(IntegerType()), True),
                        StructField("geo_altitude", StringType(), True), StructField("squawk", StringType(), True),
                        StructField("spi", StringType(), True), StructField("position_source", StringType(), True)])

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class MySparkCalculations:
    def __init__(self):
        self.states_df = self.create_df()

    def create_df(self):
        json_as_df = SPARK.read.json(JSON_PATH)
        states_list = json_as_df.select("states").first()[0]
        df = SPARK.createDataFrame(data=states_list, schema=DF_SCHEMA)
        return df

    def show_df(self, df: DataFrame):
        df.show()

    def get_highest_altitude(self):
        logging.info("Airplane(s) with highest geo altitude:")
        max_altitude_df = self.get_highest_of_column(self.states_df, column_id, column_altitude)
        self.show_df(max_altitude_df)
        return max_altitude_df

    def get_highest_velocity(self):
        logging.info("Airplane(s) with highest velocity:")
        max_velocity_df = self.get_highest_of_column(self.states_df, column_id, column_velocity)
        self.show_df(max_velocity_df)
        return max_velocity_df

    def get_highest_of_column(self, df, column1, column2_to_find_max):
        max_column2_value_df = df.select([max(column2_to_find_max)])
        max_column2_value = max_column2_value_df.first()[0]
        result_df = df.select(df[column1], df[column2_to_find_max]).filter(df[column2_to_find_max] == max_column2_value)
        return result_df

    def get_minimum_of_column(self, df, column1, column2_to_find_max):
        max_column2_value_df = df.select([min(column2_to_find_max)])
        max_column2_value = max_column2_value_df.first()[0]
        result_df = df.select(df[column1], df[column2_to_find_max]).filter(df[column2_to_find_max] == max_column2_value)
        return result_df

    def get_airplanes_count_by_airport(self):
        logging.info("Count of airplanes by airport:")
        airplanes_count_df = self.states_df.groupBy(column_country).count()
        self.show_df(airplanes_count_df)
        self.get_country_min_count_airplanes(airplanes_count_df)
        self.get_country_max_count_airplanes(airplanes_count_df)
        self.get_count_for_countries_on_c(airplanes_count_df)
        self.get_count_for_countries_on_g(airplanes_count_df)
        self.get_count_for_france_neighbours(airplanes_count_df)
        return airplanes_count_df

    def get_country_min_count_airplanes(self, airplanes_df: DataFrame):
        logging.info("Countries with smallest number of airplanes")
        min_count_airplanes_df = self.get_minimum_of_column(airplanes_df, column_country, column_count)
        self.show_df(min_count_airplanes_df)
        return min_count_airplanes_df

    def get_country_max_count_airplanes(self, airplanes_df: DataFrame):
        logging.info("Countries with largest number of airplanes")
        max_count_airplanes_df = self.get_highest_of_column(airplanes_df, column_country, column_count)
        self.show_df(max_count_airplanes_df)
        return max_count_airplanes_df

    def get_count_for_countries_on_c(self, airplanes_df: DataFrame):
        logging.info("Sum of airplanes for countries starting with C")
        sum_airplanes_c_df = self.get_count_for_countries(airplanes_df, "C")
        self.show_df(sum_airplanes_c_df)
        return sum_airplanes_c_df

    def get_count_for_countries_on_g(self, airplanes_df: DataFrame):
        logging.info("Sum of airplanes for countries starting with G")
        sum_airplanes_g_df = self.get_count_for_countries(airplanes_df, "G")
        self.show_df(sum_airplanes_g_df)
        return sum_airplanes_g_df

    def get_count_for_countries(self, df, first_letter: str):
        countries_on_g_df = df.filter(df.origin_country.startswith(first_letter))
        sum_airplanes_df = countries_on_g_df.select(
            [sum(column_count).alias("sum of airplanes from countries starting with " + first_letter)])
        return sum_airplanes_df

    def get_count_for_france_neighbours(self, airplanes_df: DataFrame):
        logging.info("Sum of airplanes of France neighbour countries:")
        france_neighbours_df = airplanes_df.filter(airplanes_df.origin_country.isin(france_neighbours))
        sum_airplanes_df = france_neighbours_df.select(
            [sum(column_count).alias("sum of airplanes of neighbours of France")])
        self.show_df(sum_airplanes_df)
        return sum_airplanes_df


if __name__ == '__main__':
    spark_task_obj = MySparkCalculations()
    spark_task_obj.get_highest_altitude()
    spark_task_obj.get_highest_velocity()
    spark_task_obj.get_airplanes_count_by_airport()
    SPARK.stop()
