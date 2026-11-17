from pathlib import Path
from common.schema_manager import load_contract, apply_schema
from common.runtime import load_run, spark_schema

def sync_layer(spark, root, layer, run):
    contract = load_contract(Path(root) / "config" / f"{layer}_schema.json")
    results = []
    for table in contract["tables"]:
        full = f"{contract['catalog']}.{contract['schema']}.{table}"
        if not spark.catalog.tableExists(full):
            if not run["schema_change_requested"] or run["schema_dry_run"]:
                raise RuntimeError(f"Missing table {full}; approved schema deployment required")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{contract['catalog']}`.`{contract['schema']}`")
            (spark.createDataFrame([], spark_schema(contract, table))
             .write.format("delta").saveAsTable(full))
        results.append(apply_schema(
            spark, contract, table,
            change_requested=run["schema_change_requested"],
            dry_run=run["schema_dry_run"],
            allow_destructive=run["allow_destructive"]))
    if run["schema_change_requested"] and run["schema_dry_run"] and any(x["steps"] for x in results):
        raise RuntimeError(f"Schema dry-run only; no pipeline writes permitted: {results}")
    return contract, results
