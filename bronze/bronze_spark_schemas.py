from pyspark.sql.types import StructType, StructField, StringType

RIDE_SCHEMA = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", StringType(), True),
    StructField("start_lng", StringType(), True),
    StructField("end_lat", StringType(), True),
    StructField("end_lng", StringType(), True),
    StructField("member_casual", StringType(), True),
])
