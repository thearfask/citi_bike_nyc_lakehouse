"""Controlled Delta schema migrations. No Spark session creation and no implicit data writes.

Supported: create absent managed tables, add nullable columns, explicit rename, explicit
metadata-only drop. Schema changes are never inferred from missing fields alone.
"""
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SQL_TYPES = {"string": "STRING", "long": "BIGINT", "integer": "INT",
             "timestamp": "TIMESTAMP", "double": "DOUBLE", "boolean": "BOOLEAN"}
SPARK_TYPES = {"string": "string", "long": "bigint", "integer": "int",
               "timestamp": "timestamp", "double": "double", "boolean": "boolean"}


def ident(value):
    if not isinstance(value, str) or not IDENT.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f"`{value}`"


def load_contract(path):
    contract = json.loads(Path(path).read_text(encoding="utf-8"))
    ident(contract["catalog"]); ident(contract["schema"])
    if contract.get("evolution_policy") != "controlled":
        raise ValueError("Only controlled evolution policy supported")
    for name, spec in contract["tables"].items():
        ident(name)
        if not isinstance(spec.get("version"), int) or spec["version"] < 1:
            raise ValueError(f"Invalid schema version: {name}")
        columns = spec["columns"] + spec.get("metadata_columns", [])
        names = [c["name"] for c in columns]
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate approved column in {name}")
        for c in columns:
            ident(c["name"])
            if c["type"] not in SQL_TYPES:
                raise ValueError(f"Unsupported type: {c['type']}")
        for c in spec["columns"]:
            if c.get("required", False) and c["type"] != "string" and spec.get("source_format") == "csv":
                raise ValueError("CSV source columns must remain string")
        seen = set()
        for m in spec.get("migrations", []):
            if m["id"] in seen:
                raise ValueError("Duplicate migration id")
            seen.add(m["id"])
            if m["operation"] not in ("rename", "drop"):
                raise ValueError(f"Unsupported migration: {m['operation']}")
            ident(m["from"] if m["operation"] == "rename" else m["column"])
            if m["operation"] == "rename": ident(m["to"])
            if m["operation"] == "drop" and m.get("confirm_data_loss") is not True:
                raise ValueError("Drop requires confirm_data_loss=true")
    return contract


def fqtn(contract, table):
    if table not in contract["tables"]:
        raise KeyError(table)
    return '.'.join(ident(x) for x in (contract["catalog"], contract["schema"], table))


def columns(contract, table):
    spec = contract["tables"][table]
    return spec["columns"] + spec.get("metadata_columns", [])


def migration_plan(spark, contract, table):
    """Pure plan, no DDL. Requires an existing table. Explicit rename/drop only."""
    full = fqtn(contract, table)
    if not spark.catalog.tableExists(full):
        raise ValueError(f"Table missing: {full}; create via reviewed bootstrap DDL")
    existing = {f.name: f.dataType.simpleString() for f in spark.table(full).schema.fields}
    desired = {c["name"]: c for c in columns(contract, table)}
    steps = []
    # Renames/drops are explicit and idempotent if their target state already exists.
    for m in contract["tables"][table].get("migrations", []):
        if m["operation"] == "rename":
            old, new = m["from"], m["to"]
            if old in existing and new in existing:
                raise ValueError(f"Ambiguous rename: both {old} and {new} exist")
            if old in existing:
                if new not in desired:
                    raise ValueError(f"Rename target not approved: {new}")
                if existing[old] != SPARK_TYPES[desired[new]["type"]]:
                    raise ValueError(f"Rename type mismatch: {old} -> {new}")
                steps.append({"operation":"rename", "from":old, "to":new})
                existing[new] = existing.pop(old)
            elif new not in existing:
                raise ValueError(f"Rename source and target both absent: {old}, {new}")
        else:
            name = m["column"]
            if name in desired:
                raise ValueError(f"Drop column still approved in contract: {name}")
            if name in existing:
                steps.append({"operation":"drop", "column":name})
                del existing[name]
    # Check every matching field before scheduling any additions.
    for name, spec in desired.items():
        if name in existing and existing[name] != SPARK_TYPES[spec["type"]]:
            raise ValueError(f"Type mismatch {full}.{name}: {existing[name]} vs {spec['type']}")
    unknown = set(existing) - set(desired)
    if unknown:
        raise ValueError(f"Unexpected existing columns: {sorted(unknown)}; explicit migration required")
    for name, spec in desired.items():
        if name not in existing:
            if not spec.get("nullable", True):
                raise ValueError(f"Cannot add NOT NULL column automatically: {name}")
            steps.append({"operation":"add", "column":name, "type":spec["type"]})
    return steps


def apply_schema(spark, contract, table, *, change_requested=False, dry_run=True,
                 allow_destructive=False):
    """Explicit per-run switch. With switch off, drift fails closed."""
    steps = migration_plan(spark, contract, table)
    result = {"table":fqtn(contract, table), "version":contract["tables"][table]["version"],
              "steps":steps, "applied":False}
    if not steps:
        return result
    if not change_requested:
        raise ValueError(f"Schema drift detected with change switch OFF: {result}")
    if any(s["operation"] == "drop" for s in steps) and not allow_destructive:
        raise ValueError("Drop requires separate allow_destructive=True authorization")
    if dry_run:
        return result
    # Delta column mapping is required for metadata-only rename/drop. Set it
    # only on a compatible table; enabling mapping may affect older readers.
    if any(s["operation"] in ("rename", "drop") for s in steps):
        props = {r.key:r.value for r in spark.sql(f"SHOW TBLPROPERTIES {result['table']}").collect()}
        if props.get("delta.columnMapping.mode") != "name":
            raise ValueError("Rename/drop requires pre-enabled Delta column mapping mode=name; "
                             "review protocol/client compatibility before enabling it")
    for s in steps:
        if s["operation"] == "rename":
            spark.sql(f"ALTER TABLE {result['table']} RENAME COLUMN {ident(s['from'])} TO {ident(s['to'])}")
        elif s["operation"] == "drop":
            spark.sql(f"ALTER TABLE {result['table']} DROP COLUMN {ident(s['column'])}")
        else:
            spark.sql(f"ALTER TABLE {result['table']} ADD COLUMNS ({ident(s['column'])} {SQL_TYPES[s['type']]})")
    # DDL is sequential and not atomic across multiple statements; rerun is idempotent.
    if migration_plan(spark, contract, table):
        raise RuntimeError("Schema migration verification failed")
    result["applied"] = True
    return result


def validate_csv_header(path, contract, table):
    with open(path, encoding="utf-8-sig", newline="") as stream:
        header = next(csv.reader(stream), None)
    if not header or len(header) != len(set(header)):
        raise ValueError(f"Empty/duplicate CSV header: {path}")
    spec = contract["tables"][table]
    source = {c["name"]:c for c in spec["columns"]}
    aliases = spec.get("source_aliases", {})
    for old, new in aliases.items():
        ident(old); ident(new)
        if new not in source:
            raise ValueError(f"Source alias target not in contract: {new}")
    normalized = [aliases.get(n, n) for n in header]
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"CSV alias collision: {path}")
    unknown = set(normalized) - set(source)
    missing = {n for n,c in source.items() if c.get("required", False)} - set(normalized)
    if unknown or missing:
        raise ValueError(f"{path}: unapproved={sorted(unknown)}, missing_required={sorted(missing)}")
    return tuple(header)


def read_approved_csvs(spark, paths, contract, table):
    """Batch reads by identical header; no collection of trip rows."""
    groups = defaultdict(list)
    for path in sorted(set(paths)):
        groups[validate_csv_header(path, contract, table)].append(path)
    result = None
    for header, group in groups.items():
        schema = StructType([StructField(n, StringType(), True) for n in header])
        df = (spark.read.format("csv").option("header", "true")
              .option("enforceSchema", "false").option("mode", "FAILFAST")
              .schema(schema).load(group)
              .withColumn("csv_path", F.regexp_replace(F.col("_metadata.file_path"), "^dbfs:", "")))
        for old, new in contract["tables"][table].get("source_aliases", {}).items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        result = df if result is None else result.unionByName(df, allowMissingColumns=True)
    return result
