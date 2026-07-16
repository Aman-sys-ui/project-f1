CREATE TABLE IF NOT EXISTS dbw_practice01.control.validation_results
(
    run_id STRING,
    entity_name STRING,
    rule_name STRING,
    rule_type STRING,
    severity STRING,
    passed BOOLEAN,
    failed_records BIGINT,
    execution_time TIMESTAMP,
    details STRING
)
USING DELTA
COMMENT 'Stores validation rule execution results for every pipeline run.';

