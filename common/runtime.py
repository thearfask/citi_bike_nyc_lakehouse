"""Small runtime helpers: config and audit writes compatible with approved contracts."""
import json
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, DoubleType, TimestampType, BooleanType)

TYPES = {"string":StringType, "long":LongType, "integer":IntegerType,
         "double":DoubleType, "timestamp":TimestampType, "boolean":BooleanType}

def load_run(path):
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    for key in ("schema_change_requested","schema_dry_run","allow_destructive","bronze_dry_run"):
        if not isinstance(cfg[key], bool):
            raise ValueError(f"{key} must be boolean")
    if cfg["max_pending_files"] < 1:
        raise ValueError("max_pending_files must be positive")
    return cfg

def spark_schema(contract, table):
    spec = contract["tables"][table]
    return StructType([StructField(c["name"], TYPES[c["type"]](), c.get("nullable", True))
                       for c in spec["columns"] + spec.get("metadata_columns", [])])

def append_audit(spark, records, contract, table):
    if not records: return
    schema = spark_schema(contract, table)
    names = {f.name for f in schema}
    unexpected = set().union(*(set(x) for x in records)) - names
    if unexpected:
        raise ValueError(f"Audit fields not in contract: {sorted(unexpected)}")
    normalized = [{f.name: record.get(f.name) for f in schema} for record in records]
    df = spark.createDataFrame(normalized, schema)
    (df.write.format("delta").mode("append").option("mergeSchema","false")
     .saveAsTable(f"{contract['catalog']}.{contract['schema']}.{table}"))
