from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)
from pyspark.sql import functions as F


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("batch_id", StringType()),
    StructField("zip_key", StringType()),
    StructField("source_key", StringType()),
    StructField("stage", StringType()),
    StructField("status", StringType()),
    StructField("source_zip_name", StringType()),
    StructField("source_url", StringType()),
    StructField("data_month", StringType()),
    StructField("target_path", StringType()),
    StructField("size_bytes", LongType()),
    StructField("file_count", IntegerType()),
    StructField("started_at", StringType()),
    StructField("finished_at", StringType()),
    StructField("error_message", StringType()),
])
FILE_SCHEMA = StructType([
    StructField("file_id", StringType()),
    StructField("extraction_event_id", StringType()),
    StructField("batch_id", StringType()),
    StructField("zip_key", StringType()),
    StructField("source_key", StringType()),
    StructField("source_zip_name", StringType()),
    StructField("source_url", StringType()),
    StructField("data_month", StringType()),
    StructField("csv_name", StringType()),
    StructField("csv_path", StringType()),
    StructField("file_size_bytes", LongType()),
    StructField("extracted_at", StringType()),
])

DISCOVERY_SCHEMA = StructType([
    StructField("zip_key", StringType()),
    StructField("source_zip_name", StringType()),
    StructField("source_url", StringType()),
    StructField("data_month", StringType()),
    StructField("zip_size_bytes", LongType()),
])