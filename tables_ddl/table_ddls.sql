CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.audit;
CREATE VOLUME IF NOT EXISTS nyc_citi_bike_catalog.audit.landing;

CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.silver;
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.gold;


CREATE TABLE IF NOT EXISTS
    nyc_citi_bike_catalog.audit.audit_events_tbl
(
    event_id       STRING NOT NULL,
    run_id         STRING NOT NULL,
    batch_id       STRING,
    stage          STRING,
    target         STRING,
    status         STRING,
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    rows_read      BIGINT,
    rows_written   BIGINT,
    error_message  STRING,

    CONSTRAINT audit_events_pk PRIMARY KEY (event_id)
)
USING DELTA;


CREATE TABLE IF NOT EXISTS
    nyc_citi_bike_catalog.audit.audit_files_tbl
(
    file_id               STRING NOT NULL,
    extraction_event_id   STRING NOT NULL,
    extraction_run_id     STRING NOT NULL,
    batch_id              STRING NOT NULL,
    source_zip_name       STRING,
    source_modified_at    TIMESTAMP,
    source_etag           STRING,
    csv_name              STRING,
    csv_path              STRING,
    file_size_bytes       BIGINT,
    extracted_at          TIMESTAMP,

    CONSTRAINT audit_files_pk PRIMARY KEY (file_id),

    CONSTRAINT audit_files_extraction_fk
        FOREIGN KEY (extraction_event_id)
        REFERENCES nyc_citi_bike_catalog.audit.audit_events_tbl (event_id)
)
USING DELTA;