-- Databricks SQL / Unity Catalog
-- Idempotent table creation; does not truncate or overwrite existing data.
-- Bronze keeps source values as strings. Parse and validate types in Silver.
-- ride_id is the source's business identifier; _source_key is file lineage only.

CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.bronze;

CREATE TABLE IF NOT EXISTS nyc_citi_bike_catalog.bronze.trips (
    ride_id STRING,
    rideable_type STRING,
    started_at STRING,
    ended_at STRING,
    start_station_name STRING,
    start_station_id STRING,
    end_station_name STRING,
    end_station_id STRING,
    start_lat STRING,
    start_lng STRING,
    end_lat STRING,
    end_lng STRING,
    member_casual STRING,
    _source_key STRING COMMENT 'Deterministic CSV identity from extraction',
    _zip_key STRING COMMENT 'Parent ZIP identity from extraction',
    _batch_id STRING COMMENT 'Original extraction batch identifier',
    _source_file STRING COMMENT 'CSV landing path',
    _ingested_at TIMESTAMP COMMENT 'Bronze ingestion timestamp'
)
USING DELTA
COMMENT 'Raw Citi Bike trip rows with file-level extraction lineage';
