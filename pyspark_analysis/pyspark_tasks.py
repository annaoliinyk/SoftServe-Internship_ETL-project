import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, min, sum
from pyspark.sql.types import *

from config import JSON_PATH, france_neighbours

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


def create_df():
    json_as_df = SPARK.read.json(JSON_PATH)
    states_list = json_as_df.select("states").first()[0]
    df = SPARK.createDataFrame(data=states_list, schema=DF_SCHEMA)
    return df


def show_df(df: DataFrame):
    df.show()


def get_highest_altitude(df: DataFrame):
    logging.info("Airplane(s) with highest geo altitude:")
    max_altitude_df = df.select([max("geo_altitude")])
    max_altitude = max_altitude_df.first()[0]
    max_altitude_df2 = df.select(df["icao24"], df["geo_altitude"]).filter(df["geo_altitude"] == max_altitude)
    show_df(max_altitude_df2)
    return max_altitude_df2


def get_highest_velocity(df: DataFrame):
    logging.info("Airplane(s) with highest velocity:")
    max_velocity_df = df.select([max("velocity")])
    max_velocity = max_velocity_df.first()[0]
    max_velocity_df2 = df.select(df["icao24"], df["velocity"]).filter(df["velocity"] == max_velocity)
    show_df(max_velocity_df2)
    return max_velocity_df2


def get_airplanes_count_by_airport(df: DataFrame):
    logging.info("Count of airplanes by airport:")
    airplanes_count_df = df.groupBy("origin_country").count()
    show_df(airplanes_count_df)
    get_country_min_count_airplanes(airplanes_count_df)
    get_country_max_count_airplanes(airplanes_count_df)
    get_count_for_countries_on_c(airplanes_count_df)
    get_count_for_countries_on_g(airplanes_count_df)
    get_count_for_france_neighbours(airplanes_count_df)
    return airplanes_count_df


def get_country_min_count_airplanes(airplanes_df: DataFrame):
    logging.info("Countries with smallest number of airplanes")
    min_count_airplanes_df = airplanes_df.select([min("count")])
    min_count_airplanes = min_count_airplanes_df.first()[0]
    min_count_airplanes_df2 = airplanes_df.select(airplanes_df["origin_country"], airplanes_df["count"]).filter(
        airplanes_df["count"] == min_count_airplanes)
    show_df(min_count_airplanes_df2)
    return min_count_airplanes_df2


def get_country_max_count_airplanes(airplanes_df: DataFrame):
    logging.info("Countries with largest number of airplanes")
    max_count_airplanes_df = airplanes_df.select([max("count")])
    max_count_airplanes = max_count_airplanes_df.first()[0]
    max_count_airplanes_df2 = airplanes_df.select(airplanes_df["origin_country"], airplanes_df["count"]).filter(
        airplanes_df["count"] == max_count_airplanes)
    show_df(max_count_airplanes_df2)
    return max_count_airplanes_df2


def get_count_for_countries_on_c(airplanes_df: DataFrame):
    logging.info("Sum of airplanes for countries starting with C")
    countries_on_c_df = airplanes_df.filter(airplanes_df.origin_country.startswith("C"))
    sum_airplanes_c_df = countries_on_c_df.select(
        [sum("count").alias("sum of airplanes from countries starting with C")])
    show_df(sum_airplanes_c_df)
    return sum_airplanes_c_df


def get_count_for_countries_on_g(airplanes_df: DataFrame):
    logging.info("Sum of airplanes for countries starting with G")
    countries_on_g_df = airplanes_df.filter(airplanes_df.origin_country.startswith("G"))
    sum_airplanes_g_df = countries_on_g_df.select(
        [sum("count").alias("sum of airplanes from countries starting with G")])
    show_df(sum_airplanes_g_df)
    return sum_airplanes_g_df


def get_count_for_france_neighbours(airplanes_df: DataFrame):
    logging.info("Sum of airplanes of France neighbour countries:")
    france_neighbours_df = airplanes_df.filter(airplanes_df.origin_country.isin(france_neighbours))
    sum_airplanes_df = france_neighbours_df.select([sum("count").alias("sum of airplanes of neighbours of France")])
    show_df(sum_airplanes_df)
    return sum_airplanes_df


if __name__ == '__main__':
    states_df = create_df()
    get_highest_altitude(states_df)
    get_highest_velocity(states_df)
    get_airplanes_count_by_airport(states_df)
    SPARK.stop()
