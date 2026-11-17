-- Optional namespace bootstrap only. Existing data tables are never dropped.
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.audit;
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.silver;
CREATE SCHEMA IF NOT EXISTS nyc_citi_bike_catalog.gold;
