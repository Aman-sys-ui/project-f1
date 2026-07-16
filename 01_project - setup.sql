-- One-time infrastructure setup (schemas, tables, storage)
-- CREATE CATALOG IF NOT EXISTS dbw_practice01 ;

USE CATALOG dbw_practice01 ;


CREATE SCHEMA IF NOT EXISTS control ;
CREATE SCHEMA IF NOT EXISTS bronze ;
CREATE SCHEMA IF NOT EXISTS silver ;
CREATE SCHEMA IF NOT EXISTS gold ;


CREATE TABLE dbw_practice01.control.ingestion_state (
    entity_name STRING NOT NULL,
    source_file STRING NOT NULL,
    file_size BIGINT NOT NULL,
    last_modified_time TIMESTAMP NOT NULL,
    content_hash STRING NOT NULL,
    status STRING NOT NULL,
    run_id STRING NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    processing_duration_ms BIGINT
)
USING DELTA
COMMENT 'Tracks processed source files for incremental ingestion.';