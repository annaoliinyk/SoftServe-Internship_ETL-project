from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import max, col
from pyspark.sql.types import *

JSON_PATH = r"C:\Users\anoliinyk\Documents\SoftServe_Internship\SoftServe-Internship_ETL-project\OpenSkyDataExtractor\all_states.json"
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


def create_df():
    json_as_df = SPARK.read.json(JSON_PATH)
    states_only_column = json_as_df.select("states").collect()[0]
    states_list = states_only_column.__getitem__("states")
    df = SPARK.createDataFrame(data=states_list, schema=DF_SCHEMA)
    return df


def show_df(df: DataFrame):
    df.show()


def get_highest_altitude(df: DataFrame):
    print("Airplane(s) with highest geo altitude:")
    max_altitude_df = df.withColumn("icao24", col("icao24")).agg(F.max("geo_altitude").alias("geo_altitude"))
    show_df(max_altitude_df)


def get_highest_velocity(df: DataFrame):
    print("\nAirplane(s) with highest velocity:")
    max_velocity_df = df.select([max("velocity")])
    show_df(max_velocity_df)


def get_airplanes_count_by_airport(df: DataFrame):
    print("\nCount of airplanes by airport:")
    airplanes_count_df = df.groupBy("origin_country").count()
    show_df(airplanes_count_df)


states_df = create_df()
get_highest_altitude(states_df)
get_highest_velocity(states_df)
get_airplanes_count_by_airport(states_df)
SPARK.stop()
