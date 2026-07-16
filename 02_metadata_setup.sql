-- Drop and recreate the catalog to clear the stale storage credential
-- from the old workspace. The new catalog inherits the UC metastore
-- default managed storage (dbstorageulmihqt42xjsk) automatically.
-- DROP CATALOG IF EXISTS dbw_practice01 CASCADE;
CREATE CATALOG IF NOT EXISTS dbw_practice01;

USE CATALOG dbw_practice01;

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Populate entity_config metadata table
CREATE OR REPLACE TABLE dbw_practice01.control.entity_config
(
    entity_name      STRING,
    source_format    STRING,
    source_path      STRING,
    target_schema    STRING,
    target_table     STRING,
    load_type        STRING,
    partition_column STRING,
    schema_file      STRING,
    active           BOOLEAN,
    created_ts       TIMESTAMP,
    multiline        BOOLEAN
)
USING DELTA;

INSERT INTO dbw_practice01.control.entity_config VALUES

-- circuits: CSV, multiline not applicable
('circuits',     'csv',  'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/circuits/',     'bronze', 'circuits',     'full',        NULL,        'circuits_schema',     true, current_timestamp(), NULL),
-- constructors: NDJSON (one object per line)
('constructors', 'json', 'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/constructors/', 'bronze', 'constructors', 'full',        NULL,        'constructors_schema', true, current_timestamp(), false),
-- drivers: NDJSON (one object per line)
('drivers',      'json', 'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/drivers/',      'bronze', 'drivers',      'full',        NULL,        'drivers_schema',      true, current_timestamp(), false),
-- races: CSV, multiline not applicable
('races',        'csv',  'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/races/',        'bronze', 'races',        'full',        NULL,        'races_schema',        true, current_timestamp(), NULL),
-- results: JSON array (multiline required)
('results',      'json', 'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/results/',      'bronze', 'results',      'incremental', 'race_year', 'results_schema',      true, current_timestamp(), true),
-- sprints: JSON array (multiline required)
('sprints',      'json', 'abfss://raw@devstorageaccountpc01.dfs.core.windows.net/sprints/',      'bronze', 'sprints',      'incremental', 'race_year', 'sprints_schema',      true, current_timestamp(), true);
