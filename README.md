# Citi Bike Lakehouse — full Extraction + Bronze test candidate

Based on the GitHub `dev` branch inspected November 16, 2026:
`https://github.com/thearfask/citi_bike_nyc_lakehouse/tree/dev`

**Contains actual extraction code** copied from the local copy of the dev branch's
`extraction/extract.py` (same 10,835-byte source inspected), integrated extraction
and Bronze notebooks, global schema engine, and four renamed JSON contracts.

**Status:** syntax-checked locally; **not executed against Databricks**. This is a
complete candidate for *staged testing*, not a certified production deployment.
Silver and Gold data transformations do not exist in the upstream repo; their
schema contracts intentionally start empty.

## Compact layout

- `config/audit_schema.json`: both audit tables
- `config/bronze_schema.json`: Bronze trip table
- `config/silver_schema.json`, `gold_schema.json`: future table contracts
- `config/run.json`: global schema-change and dry-run switches
- `common/schema_manager.py`: compare/plan/apply Delta DDL, validate CSV headers
- `common/runtime.py`, `common/execute.py`: typed audit writes and orchestration
- `common/00_schema_check.ipynb`: inspect/apply all four layer contracts
- `extraction/extract.py`: real S3 ZIP discovery, download, unzip, lineage JSON
- `extraction/config.json`: original settings, corrected `tun_type` typo
- `extraction/01_extract.ipynb`: actual extraction pipeline + audit writes
- `bronze/02_bronze.ipynb`: pending-file discovery, CSV header validation,
  grouped distributed read, source_key-scoped replaceWhere, audit SUCCESS
- `sql/setup.sql`: schemas only
- `tests/`: offline contract/header checks

## Important compatibility warning — read before running

The upstream repo contains **conflicting audit definitions**:
`extraction/spark_schemas.py` defines `zip_key`, `source_key`, `source_zip_name`,
etc., but `tables_ddl/table_ddls.sql` defines a different table with `run_id`,
`target`, and other fields. This package uses the **actual extraction notebook's
Spark schemas**, plus optional `rows_read` and `rows_written`. You MUST inspect
`DESCRIBE TABLE nyc_citi_bike_catalog.audit.audit_events_tbl` and
`...audit_files_tbl` on Databricks. The manager rejects unexpected columns and
incompatible types rather than altering them silently.

The upstream `bronze/bronze_tables.sql` defines `bronze.trips` while the working
notebook writes `bronze.raw_trips_tbl`. This package uses `raw_trips_tbl` and
its actual notebook lineage fields `source_key`, `batch_id`.

Existing January–March data is never intentionally dropped. **Test against
clones or a separate test catalog first.** The first test should not target the
existing production table.

## Recommended staged test

1. Clone your Delta tables into a test catalog and update the four contract
   `catalog` values and Bronze notebook table names accordingly. Also update
   `extraction/config.json` audit table references and landing volume.
2. Run `DESCRIBE TABLE` on the two audit tables and Bronze target. Reconcile
   their JSON definitions exactly. Do not guess types or nullability.
3. Start with `schema_change_requested=false`, `schema_dry_run=true`,
   `bronze_dry_run=true`. Run schema check; it should report no changes.
4. To add a column, update `bronze_schema.json` columns, bump table version,
   set `schema_change_requested=true`, `schema_dry_run=true`; inspect plan.
5. Set `schema_dry_run=false` to apply approved DDL. Re-run; plan should be empty.
6. Test CSV containing the approved column, then test an unapproved column.
   The latter must fail before the Bronze write.
7. Set `bronze_dry_run=false` only after validating file counts and lineage.
8. Check audit events, Bronze rows, and rerun behavior.

## Global schema-change switch

`config/run.json` controls all layers:
- `schema_change_requested=false`: strict validation; no DDL
- `schema_change_requested=true`, `schema_dry_run=true`: migration plan, no writes
- `schema_change_requested=true`, `schema_dry_run=false`: approved DDL applies
- `allow_destructive=true`: additional authorization for explicit column drops

**Rename/drop:** declare an explicit `migrations` entry on the relevant table
and change the desired columns. Rename/drop on Delta requires column mapping
mode `name`, configured separately after reviewing reader/protocol compatibility.
The manager refuses to enable column mapping automatically.

Example:
```json
"migrations": [{"id":"2026_11_rename","operation":"rename","from":"rideable_type","to":"vehicle_type"}]
```
Also update the column name in `columns`. For CSV sources where the upstream
header remains `rideable_type`, add:
```json
"source_aliases": {"rideable_type":"vehicle_type"}
```
For a drop:
```json
"migrations": [{"id":"2026_11_drop","operation":"drop","column":"old_field","confirm_data_loss":true}]
```
Remove the field from desired columns. Drops are logical/metadata-only with
Delta column mapping; physical bytes may persist until a reviewed rewrite/
maintenance procedure. Downstream consumers and historical replays need explicit
compatibility work.

## Recovery and concurrency

Bronze writes use bounded `replaceWhere` over the pending source keys. A
repeated write of the same file set replaces its previous rows instead of
appending duplicates; the audit SUCCESS is written only after the Delta commit.
A crash between data and audit can be retried safely at the Bronze data layer.
However, **cross-table audit and data commits are not atomic**, and concurrent
writers must be serialized by orchestration. Audit event duplication on
repeated extraction after partial audit writes remains a known recovery gap.

## Limits

- Source column type changes and non-null additions are rejected.
- CSV header validation occurs on driver for bounded file metadata; Spark
  distributes actual CSV processing.
- `bronze_dry_run=true` by default.
- The first migration against an existing table may require reconciling JSON
  with the actual deployed Delta schema. This is intentional fail-closed behavior.
- Silver/Gold *schema management* works when their table contracts are defined,
  but their ETL transformations have not been invented.
- A global switch is an **execution control**, not proof of business approval.
  JSON contract changes must be reviewed in Git.
