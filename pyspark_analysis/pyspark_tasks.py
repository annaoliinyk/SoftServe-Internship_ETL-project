from pyspark.sql import SparkSession
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


def show_df(df):
    df.show()


def get_highest_altitude(df):
    max_altitude = df.agg({"geo_altitude": "max"}).collect()[0]
    return max_altitude


def get_highest_velocity(df):
    max_velocity = df.agg({"velocity": "max"}).collect()[0]
    return max_velocity


states_df = create_df()
show_df(states_df)
print(get_highest_altitude(states_df))
print(get_highest_velocity(states_df))
SPARK.stop()
