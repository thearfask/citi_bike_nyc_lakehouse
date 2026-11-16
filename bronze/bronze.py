"""Small reusable helpers for Citi Bike Bronze ingestion.

No Spark session creation, file discovery, audit queries, Delta writes, or
orchestration here: the Databricks notebook owns those operations.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Preserve source values as strings; type conversions and business rules belong in Silver.
TRIP_COLUMNS = (
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
)


def trip_schema() -> StructType:
    """Explicit source schema; all raw CSV fields remain strings."""
    return StructType([StructField(name, StringType(), True) for name in TRIP_COLUMNS])


def read_trip_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    """Read exactly one extracted CSV, retaining duplicates and raw string values.

    FAILFAST avoids silently accepting malformed records. The notebook should
    validate the source header/schema and handle errors before committing data.
    """
    return (
        spark.read
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .schema(trip_schema())
        .csv(csv_path)
    )


def add_lineage(df: DataFrame, file_record: dict) -> DataFrame:
    """Add extraction lineage to every row of one CSV.

    file_record comes from audit_files_tbl. Do not use source_key as a trip ID:
    multiple rides in the same CSV share it; ride_id is the business identifier.
    """
    required = ("source_key", "zip_key", "batch_id", "csv_path")
    missing = [key for key in required if not file_record.get(key)]
    if missing:
        raise ValueError(f"Missing audit file metadata: {', '.join(missing)}")

    return (
        df.withColumn("_source_key", F.lit(file_record["source_key"]))
          .withColumn("_zip_key", F.lit(file_record["zip_key"]))
          .withColumn("_batch_id", F.lit(file_record["batch_id"]))
          .withColumn("_source_file", F.lit(file_record["csv_path"]))
          .withColumn("_ingested_at", F.current_timestamp())
    )
