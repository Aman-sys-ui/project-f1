# Formula 1 Data Ingestion Framework

A metadata-driven data ingestion framework built on PySpark and Databricks, implementing the Medallion Architecture. The framework replaces entity-specific pipelines with a single, configuration-driven engine capable of ingesting any structured source into a governed Delta Lakehouse.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Directory Structure](#directory-structure)
- [Execution Lifecycle](#execution-lifecycle)
- [Metadata-Driven Design](#metadata-driven-design)
- [Framework Components](#framework-components)
- [Control Tables](#control-tables)
- [Current Features](#current-features)
- [Design Principles](#design-principles)
- [Tech Stack](#tech-stack)
- [Future Roadmap](#future-roadmap)

---

## Architecture Overview

The framework follows a layered architecture where each module has a single responsibility. The ingestion engine orchestrates the pipeline by delegating to specialized components:

```
                    +---------------------+
                    |  Bronze Runner      |
                    |  (Notebook)         |
                    +----------+----------+
                               |
                               v
                    +---------------------+
                    |  Ingestion Engine   |
                    |  (Orchestrator)     |
                    +----------+----------+
                               |
    +-------+-------+----------+----------+-------+-------+
    |       |       |          |          |       |       |
    v       v       v          v          v       v       v
+------+ +------+ +------+ +------+ +------+ +-----+ +-----+
|Config| |Schema| |Reader| |Vali- | |Writer| | DQ  | |State|
|Loader| |Loader| |      | |dation| |      | |     | | Mgr |
+------+ +------+ +------+ +------+ +------+ +-----+ +-----+
                                                         |
                          +------------------------------+
                          |
    +----------+----------+----------+----------+
    |          |          |          |          |
    v          v          v          v          v
+---------+ +--------+ +--------+ +--------+ +--------+
| Audit   | | Water- | |Pipeline| | Excep- | | Retry  |
| Logger  | | mark   | |Run Mgr | | tions  | | Frmwk  |
+---------+ +--------+ +--------+ +--------+ +--------+


                    +---------------------+
                    |  Silver Engine      |
                    |  (Orchestrator)     |
                    +----------+----------+
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
    +------------------+ +-----------+ +----------------+
    | Transformation   | | Transform | | Silver         |
    | Registry         | | Loader    | | Validators     |
    +------------------+ +-----------+ +----------------+
              |
    +---------+---------+---------+---------+---------+
    |         |         |         |         |         |
    v         v         v         v         v         v
+------+ +------+ +------+ +------+ +------+ +------+
|circui| |const-| |drive-| |races | |resul-| |sprin-|
|ts    | |ructrs| |rs    | |      | |ts    | |ts    |
+------+ +------+ +------+ +------+ +------+ +------+
```

**Data flow direction:**

```
Azure Data Lake (Raw)  --->  Bronze (Delta)  --->  Silver (Delta)  --->  Gold (Delta)
      [CSV/JSON]             [Append/Overwrite]     [Cleansed/Typed]     [Aggregates]
```

All layers are registered in Unity Catalog under the `dbw_practice` catalog with dedicated schemas: `control`, `bronze`, `silver`, `gold`.

---

## Directory Structure

```
project-f1/
|
|-- configs/
|   |-- environments.yml          # Environment-specific settings (dev, staging, prod)
|   |-- entities.yml              # Entity configuration reference
|
|-- notebooks/
|   |-- bronze/
|       |-- bronze_runner          # Execution notebook that invokes the engine per entity
|
|-- src/
|   |-- common/
|   |   |-- __init__.py
|   |   |-- config_loader.py      # Reads entity configuration from Unity Catalog table
|   |   |-- constants.py          # Centralized constants (paths, table names, load types)
|   |   |-- exceptions.py         # Custom exception hierarchy for structured error handling
|   |   |-- logger.py             # Logging configuration
|   |   |-- models.py             # Shared data models (ValidationResult, DQResult)
|   |   |-- retry.py              # Decorator-based retry framework with configurable policies
|   |   |-- state_manager.py      # File-level change detection and ingestion state tracking
|   |   |-- utils.py              # Shared utility functions
|   |
|   |-- ingestion/
|   |   |-- __init__.py
|   |   |-- bronze_ingestion_engine.py  # Core orchestrator: drives the full ingestion lifecycle
|   |   |-- bronze_writer.py            # Writes DataFrames to Bronze Delta tables (with retry)
|   |   |-- reader.py                   # Reads source data with format and schema awareness (with retry)
|   |   |-- schema_loader.py            # Maps entity names to PySpark StructType definitions
|   |
|   |-- validation/
|   |   |-- validation_runner.py        # Orchestrates pre-write validation execution
|   |   |-- validation_engine.py        # Executes validation rules, collects results
|   |   |-- validation_rules.py         # Rule implementations: schema, required columns, not-null
|   |   |-- rule_loader.py             # Loads enabled rules from validation_rules metadata table
|   |
|   |-- dq/
|   |   |-- __init__.py
|   |   |-- dq_runner.py               # Orchestrates post-write DQ execution
|   |   |-- dq_engine.py               # Executes DQ rules, severity-aware pass/fail
|   |   |-- dq_rules.py               # Rule implementations (7 rule types)
|   |   |-- dq_rule_loader.py         # Loads enabled rules from dq_rules metadata table
|   |   |-- checks.py                 # Legacy DQ check definitions
|   |
|   |-- silver/
|   |   |-- __init__.py
|   |   |-- silver_engine.py            # Silver layer orchestrator (metadata-driven)
|   |   |-- transformation_registry.py  # Maps transformation names to Python functions
|   |   |-- transformation_loader.py    # Resolves transformation name from metadata to callable
|   |   |-- validator.py                # Silver-level validation coordinator
|   |   |-- transformations/
|   |   |   |-- __init__.py
|   |   |   |-- circuits.py            # transform_circuits: circuit entity cleansing
|   |   |   |-- constructors.py        # transform_constructors: constructor entity cleansing
|   |   |   |-- drivers.py             # transform_drivers: driver entity cleansing
|   |   |   |-- races.py               # transform_races: race entity cleansing
|   |   |   |-- results.py             # transform_results: results entity cleansing
|   |   |   |-- sprints.py             # transform_sprints: sprints entity cleansing
|   |   |-- validators/
|   |       |-- __init__.py
|   |       |-- schema_validator.py     # Silver schema conformance checks
|   |       |-- business_validator.py   # Business rule validation for silver output
|   |
|   |-- gold/
|   |   |-- __init__.py
|   |   |-- driver_standings.py    # Driver standings aggregation
|   |   |-- constructor_standings.py  # Constructor standings aggregation
|   |
|   |-- monitoring/
|       |-- __init__.py
|       |-- audit_logger.py             # Writes audit records and watermark updates (with retry)
|       |-- pipeline_run_manager.py     # Pipeline run lifecycle (start, complete, fail)
|
|-- 01.project - setup             # One-time infrastructure setup (schemas, tables, storage)
|-- 02_metadata_setup              # Populates entity_config metadata table
|-- 03_schema_discovery            # Schema inference and documentation
|-- README.md
```

| Directory | Responsibility |
| --- | --- |
| `configs/` | Static YAML configuration files for environment and entity metadata |
| `src/common/` | Shared infrastructure: configuration, constants, exceptions, models, retry, state management |
| `src/ingestion/` | Bronze layer pipeline: engine, reader, writer, schema definitions |
| `src/validation/` | Pre-write validation: schema checks, required columns, not-null enforcement |
| `src/dq/` | Post-write data quality: duplicate detection, value ranges, regex, null thresholds |
| `src/silver/` | Silver layer: engine, transformation registry, per-entity transformations, validators |
| `src/gold/` | Gold layer aggregations: business-level KPIs and standings |
| `src/monitoring/` | Observability: audit logs, pipeline run tracking, watermarks |
| `notebooks/` | Execution entry points that invoke the framework |

---

## Execution Lifecycle

The ingestion engine follows a deterministic lifecycle for every entity invocation:

```
START
  |
  v
[1] Pipeline Run Started
  |   - Generate UUID run_id
  |   - Record RUNNING status in pipeline_runs table
  |
  v
[2] Load Configuration
  |   - Read entity_config from Unity Catalog
  |   - Extract source_path, format, schema_file, load_type, partition_column
  |
  v
[3] Load Schema
  |   - Resolve schema_file to a PySpark StructType
  |   - Apply schema-on-read to enforce structure
  |
  v
[4] Determine Write Mode
  |   - full   -> overwrite
  |   - incremental -> append
  |
  v
[5] Change Detection (incremental only)
  |   - Check if target table exists; reset state if missing
  |   - Read current ingestion_state for entity
  |   - Scan source files: path, modification time, size, content hash
  |   - Compute delta: identify new or modified files
  |   - If no changes detected -> mark SKIPPED, exit early
  |
  v
[6] Read Source (with retry)
  |   - Read from ADLS using format-specific reader (CSV/JSON)
  |   - Filter to only include files requiring processing
  |
  v
[7] Pre-Write Validation
  |   - Load validation rules from metadata table
  |   - Execute: schema checks, required columns, not-null
  |   - Persist results to validation_results table
  |   - If any ERROR severity rule fails -> raise ValidationException
  |
  v
[8] Add Partition Column
  |   - Extract partition value (e.g., race_year from date)
  |
  v
[9] Add Metadata Columns
  |   - _ingestion_ts: processing timestamp
  |   - _source_file: originating file path
  |   - _pipeline_run_id: correlation to run lifecycle
  |
  v
[10] Validate Record Count
  |   - Cache DataFrame, compute count
  |   - If 0 records -> mark SKIPPED, exit early
  |
  v
[11] Write Bronze (with retry)
  |   - Write Delta to ADLS Bronze container
  |   - Register table in Unity Catalog if not exists
  |   - Apply partition scheme if configured
  |
  v
[12] Update Ingestion State
  |   - MERGE processed file metadata into ingestion_state
  |   - Records: source_file, file_size, modification_time, content_hash
  |
  v
[13] Write Audit Log (with retry)
  |   - Append run_id, entity, record_count, status to audit_log
  |
  v
[14] Update Watermark
  |   - MERGE max(last_modified_time) into watermarks table
  |
  v
[15] Complete Pipeline Run
  |   - UPDATE pipeline_runs: status=SUCCESS, duration_ms, record/file counts
  |
  v
END (return fully qualified table name)
```

**Error Path:** If any step raises a `PipelineException` (or subclass), the engine catches it, logs the failure via `fail_run()`, records `FAILED` status with the error message in `pipeline_runs`, and re-raises. Unexpected exceptions follow the same path with an "Unexpected pipeline failure" log entry.

**Retry Behavior:** Components decorated with `@retry` (Reader, Writer, Audit Logger) automatically retry on their respective exception types with configurable attempt count and delay.

---

## Metadata-Driven Design

The framework eliminates hardcoded pipeline logic by externalizing all entity-specific behavior into metadata tables. Adding a new data source requires only an `INSERT` into `entity_config` and a schema definition -- no code changes. Validation and DQ rules are similarly metadata-driven. Silver transformations are resolved via a transformation registry keyed by metadata.

### Why Metadata-Driven?

| Concern | Traditional Approach | This Framework |
| --- | --- | --- |
| Adding a new source | Write a new pipeline | Insert a config row |
| Changing load strategy | Modify and redeploy code | Update a column value |
| Partition scheme | Hardcoded per pipeline | Configured per entity |
| Schema enforcement | Scattered across pipelines | Centralized schema registry |
| Validation rules | Hardcoded assertions | Metadata-driven rule execution |
| Data quality checks | Custom per-entity scripts | Centralized DQ rules table |
| Silver transformations | Hardcoded per entity | Registry-resolved via metadata |
| Observability | Custom logging per pipeline | Unified audit/watermark/run tracking |

### Control Table Roles

| Table | Purpose |
| --- | --- |
| `entity_config` | Source of truth for every ingestion target: paths, formats, schemas, load strategies |
| `pipeline_runs` | Full lifecycle tracking: start, end, duration, status, error messages |
| `audit_log` | Immutable append-only record of every successful ingestion with record counts |
| `ingestion_state` | File-level change detection state: tracks which files have been processed |
| `watermarks` | High-water mark per entity: the latest modification timestamp processed |
| `validation_rules` | Pre-write validation rule definitions per entity |
| `validation_results` | Validation execution outcomes with pass/fail and details |
| `dq_rules` | Post-write data quality rule definitions per entity |
| `dq_results` | DQ execution outcomes with severity-aware pass/fail |

---

## Framework Components

### Config Loader (`src/common/config_loader.py`)

Reads a single row from `entity_config` for the requested entity. Returns a dictionary containing all configuration keys needed by downstream components. Raises `ConfigurationException` if the entity is not found or the table is unreachable.

### Schema Loader (`src/ingestion/schema_loader.py`)

Maintains a registry of PySpark `StructType` definitions keyed by `schema_file` name. The engine calls `get_schema(schema_name)` to resolve configuration to a concrete schema. Enforces schema-on-read to catch structural drift at ingestion time rather than downstream.

### Reader (`src/ingestion/reader.py`)

Format-aware source reader supporting CSV and JSON. Accepts an optional `files_to_include` parameter for incremental loads -- when provided, filters the read DataFrame to only include rows from specified file paths using `_metadata.file_path`. Decorated with `@retry` for transient failure recovery. Raises `ReadException` on failure.

### Bronze Writer (`src/ingestion/bronze_writer.py`)

Writes DataFrames to the Bronze ADLS container as Delta format. Supports partitioned writes and registers external tables in Unity Catalog via `CREATE TABLE IF NOT EXISTS ... LOCATION`. Decorated with `@retry` for transient failure recovery. Raises `WriteException` on failure.

### Validation Framework (`src/validation/`)

Pre-write validation that runs between source read and Bronze write. Architecture:

| Component | Responsibility |
| --- | --- |
| `rule_loader.py` | Loads enabled rules from `validation_rules` table for the entity |
| `validation_rules.py` | Implements rule logic: `validate_schema`, `validate_required_columns`, `validate_not_null` |
| `validation_engine.py` | Executes rules, collects `ValidationResult` objects, reports pass/fail |
| `validation_runner.py` | Orchestrates the full flow: load rules, execute, persist results, return outcome |

Validation results are persisted to `validation_results` regardless of pass/fail. If any ERROR-severity rule fails, a `ValidationException` is raised, halting the pipeline before any data is written.

### Data Quality Framework (`src/dq/`)

Post-write data quality checks that can run independently or as part of the pipeline. Implements 7 rule types:

| Rule Type | Function | Description |
| --- | --- | --- |
| `DUPLICATE_KEY` | `validate_duplicate_keys` | Business key uniqueness via self-join on duplicates |
| `DUPLICATE_ROW` | `validate_duplicate_rows` | Full row deduplication check |
| `ALLOWED_VALUES` | `validate_allowed_values` | Column values within an enumerated set |
| `REGEX` | `validate_regex` | Column values match a regular expression pattern |
| `NUMERIC_RANGE` | `validate_numeric_range` | Numeric values within inclusive min/max bounds |
| `DATE_RANGE` | `validate_date_range` | Date values within inclusive date bounds |
| `NULL_PERCENTAGE` | `validate_null_percentage` | NULL ratio below a configurable threshold |

Architecture mirrors the validation framework:

| Component | Responsibility |
| --- | --- |
| `dq_rule_loader.py` | Loads enabled rules from `dq_rules` table for the entity |
| `dq_rules.py` | Implements all 7 rule types, each returning a `DQResult` |
| `dq_engine.py` | Executes rules; `passed()` only fails on ERROR severity (WARNINGs are logged, not blocking) |
| `dq_runner.py` | Orchestrates: load rules, execute, persist to `dq_results`, return outcome |

### Silver Layer (`src/silver/`)

Metadata-driven transformation framework for cleansing and typing Bronze data into Silver. Architecture follows the same registry pattern as validation and DQ:

| Component | Responsibility |
| --- | --- |
| `silver_engine.py` | Orchestrates Silver processing: load config, resolve transformation, validate, write |
| `transformation_registry.py` | Maps transformation names to callable Python functions |
| `transformation_loader.py` | Resolves a transformation name from metadata to its registered function |
| `validator.py` | Coordinates Silver-level validation before write |
| `validators/schema_validator.py` | Validates output DataFrame conforms to expected Silver schema |
| `validators/business_validator.py` | Validates business rules (e.g., referential integrity, value constraints) |
| `transformations/*.py` | Per-entity transformation functions (one file per entity) |

**Transformation Registry Pattern:**

```python
TRANSFORMATION_REGISTRY = {
    "transform_drivers": transform_drivers,
    "transform_constructors": transform_constructors,
    "transform_circuits": transform_circuits,
    "transform_races": transform_races,
    "transform_results": transform_results,
    "transform_sprints": transform_sprints,
}
```

Adding a new Silver transformation requires:
1. Create `src/silver/transformations/<entity>.py` with a `transform_<entity>(df)` function
2. Register it in `transformation_registry.py`
3. No changes to the engine or loader

### Retry Framework (`src/common/retry.py`)

Decorator-based retry mechanism applied to components susceptible to transient failures:

```python
@retry(retries=3, delay=5, retry_on=(ReadException,), logger=logger)
def read_source(...):
    ...
```

| Parameter | Description |
| --- | --- |
| `retries` | Maximum number of attempts before propagating the exception |
| `delay` | Seconds to wait between retry attempts |
| `retry_on` | Tuple of exception types that trigger a retry |
| `logger` | Optional logger for retry attempt messages |

Currently applied to: Reader (3 retries, 5s delay), Writer (2 retries, 10s delay), Audit Logger (2 retries, 2s delay).

### State Manager (`src/common/state_manager.py`)

Implements file-level change detection using a four-signal approach:

| Signal | Detection |
| --- | --- |
| File path | New file never seen before |
| Modification timestamp | File touched since last run |
| File size | Content length changed |
| Content hash (MD5) | Catches same-size content replacements |

Provides `get_source_file_info()` which performs a single `binaryFile` read to extract all four signals in one pass. Uses MERGE-based upserts to maintain state. Raises `StateException` on any state operation failure.

**Auto-recovery:** If the target Bronze table is dropped, the state manager detects the missing table and resets all tracking state, forcing a full re-ingestion on the next run.

### Audit Logger (`src/monitoring/audit_logger.py`)

Appends immutable audit records after every successful write. Also manages watermark updates via MERGE upserts into the `watermarks` table. Decorated with `@retry`. Raises `AuditException` on failure.

### Pipeline Run Manager (`src/monitoring/pipeline_run_manager.py`)

Manages the full run lifecycle:

- `start_run()` -- Creates a RUNNING record, returns `(run_id, start_time)`
- `complete_run()` -- Updates to SUCCESS/SKIPPED with duration and metrics
- `fail_run()` -- Updates to FAILED with error message

Every invocation of `run()` is bracketed by `start_run` / `complete_run` or `fail_run`, ensuring no run is left in an indeterminate state.

### Exception Framework (`src/common/exceptions.py`)

Structured exception hierarchy for precise error classification:

```
PipelineException (base)
    |-- ConfigurationException   # Invalid or missing configuration
    |-- SchemaException          # Schema loading or validation failure
    |-- ReadException            # Source data cannot be read
    |-- ValidationException      # Data validation failure
    |-- WriteException           # Target write failure
    |-- StateException           # Ingestion state update failure
    |-- AuditException           # Audit or watermark logging failure
```

The engine catches `PipelineException` for known failures and `Exception` for unexpected errors, ensuring both paths record the failure in `pipeline_runs` before re-raising.

---

## Control Tables

### `dbw_practice.control.entity_config`

| Column | Type | Description |
| --- | --- | --- |
| entity_name | STRING | Unique identifier (e.g., `circuits`, `results`) |
| source_format | STRING | File format: `csv`, `json` |
| source_path | STRING | ADLS path to source data |
| target_schema | STRING | Target schema (e.g., `bronze`) |
| target_table | STRING | Target table name |
| load_type | STRING | `full` or `incremental` |
| partition_column | STRING | Optional partition column name |
| schema_file | STRING | Key into the schema registry |
| is_active | BOOLEAN | Enable/disable entity processing |
| incremental_strategy | STRING | `file_tracking` or NULL |

### `dbw_practice.control.pipeline_runs`

| Column | Type | Description |
| --- | --- | --- |
| run_id | STRING | UUID for the pipeline execution |
| pipeline_name | STRING | Pipeline identifier (e.g., `bronze_ingestion`) |
| entity_name | STRING | Entity being processed |
| start_time | TIMESTAMP | Run start timestamp |
| end_time | TIMESTAMP | Run completion timestamp |
| duration_ms | BIGINT | Total execution duration in milliseconds |
| status | STRING | `RUNNING`, `SUCCESS`, `FAILED`, `SKIPPED` |
| records_processed | BIGINT | Number of records written |
| files_processed | BIGINT | Number of files ingested |
| error_message | STRING | Error details on failure |
| created_by | STRING | Execution identity |

### `dbw_practice.control.ingestion_state`

| Column | Type | Description |
| --- | --- | --- |
| entity_name | STRING | Entity identifier |
| source_file | STRING | Full ADLS file path |
| file_size | BIGINT | File size in bytes |
| last_modified_time | TIMESTAMP | File modification timestamp |
| content_hash | STRING | MD5 hash of file content |
| status | STRING | Processing status |
| run_id | STRING | Correlation to pipeline_runs |
| processed_at | TIMESTAMP | When the file was processed |
| processing_duration_ms | BIGINT | Processing time for this file batch |

### `dbw_practice.control.watermarks`

| Column | Type | Description |
| --- | --- | --- |
| entity_name | STRING | Entity identifier |
| watermark_value | STRING | Latest processed modification timestamp |
| updated_ts | TIMESTAMP | When the watermark was last updated |

### `dbw_practice.control.audit_log`

| Column | Type | Description |
| --- | --- | --- |
| run_id | STRING | Correlation to pipeline_runs |
| entity_name | STRING | Entity identifier |
| record_count | BIGINT | Records ingested in this run |
| status | STRING | Outcome status |
| created_ts | TIMESTAMP | Audit record creation time |

### `dbw_practice.control.validation_rules`

| Column | Type | Description |
| --- | --- | --- |
| entity_name | STRING | Entity this rule applies to |
| rule_name | STRING | Human-readable rule identifier |
| rule_type | STRING | `SCHEMA`, `REQUIRED_COLUMN`, `NOT_NULL` |
| column_name | STRING | Target column (NULL for schema-level rules) |
| rule_value | STRING | Rule parameter (unused for basic validation rules) |
| severity | STRING | `ERROR` (blocks pipeline) or `WARNING` (logged only) |
| enabled | BOOLEAN | Enable/disable rule without deletion |

### `dbw_practice.control.validation_results`

| Column | Type | Description |
| --- | --- | --- |
| run_id | STRING | Correlation to pipeline_runs |
| entity_name | STRING | Entity validated |
| rule_name | STRING | Rule that was executed |
| rule_type | STRING | Rule type identifier |
| severity | STRING | ERROR or WARNING |
| passed | BOOLEAN | Whether the rule passed |
| failed_records | BIGINT | Count of records that violated the rule |
| execution_time | TIMESTAMP | When the validation ran |
| details | STRING | Human-readable failure description |

### `dbw_practice.control.dq_rules`

| Column | Type | Description |
| --- | --- | --- |
| entity_name | STRING | Entity this rule applies to |
| rule_name | STRING | Human-readable rule identifier |
| rule_type | STRING | `DUPLICATE_KEY`, `DUPLICATE_ROW`, `ALLOWED_VALUES`, `REGEX`, `NUMERIC_RANGE`, `DATE_RANGE`, `NULL_PERCENTAGE` |
| column_name | STRING | Target column |
| rule_value | STRING | Rule parameter (e.g., `"0,100"` for range, `"M,F,U"` for allowed values) |
| threshold | FLOAT | Threshold parameter (e.g., max null percentage) |
| severity | STRING | `ERROR` (blocks pipeline) or `WARNING` (logged only) |
| enabled | BOOLEAN | Enable/disable rule without deletion |

### `dbw_practice.control.dq_results`

| Column | Type | Description |
| --- | --- | --- |
| run_id | STRING | Correlation to pipeline_runs |
| entity_name | STRING | Entity checked |
| rule_name | STRING | Rule that was executed |
| rule_type | STRING | Rule type identifier |
| severity | STRING | ERROR or WARNING |
| passed | BOOLEAN | Whether the rule passed |
| failed_records | BIGINT | Count of records that violated the rule |
| execution_time | TIMESTAMP | When the DQ check ran |
| details | STRING | Human-readable failure description |

---

## Current Features

- [x] Metadata-driven ingestion from Unity Catalog configuration table
- [x] Dynamic schema enforcement via centralized schema registry
- [x] Multi-format source support (CSV, JSON)
- [x] Full load (overwrite) and incremental load (append) strategies
- [x] File-level change detection (path, size, timestamp, content hash)
- [x] Automatic state recovery when target tables are dropped
- [x] Idempotent re-runs (no reprocessing when files unchanged)
- [x] Partitioned Delta writes (e.g., `race_year`)
- [x] Metadata column enrichment (`_ingestion_ts`, `_source_file`, `_pipeline_run_id`)
- [x] Pipeline run lifecycle tracking (start, complete, fail)
- [x] Watermark management for incremental high-water mark
- [x] Immutable audit logging
- [x] Structured exception hierarchy with typed exceptions per component
- [x] Unity Catalog table registration
- [x] Delta Lake storage format across all layers
- [x] Azure Data Lake Storage Gen2 integration
- [x] Configurable source paths and target locations
- [x] Retry framework with decorator-based configurable policies
- [x] Pre-write validation framework (schema, required columns, not-null)
- [x] Post-write Data Quality framework (7 rule types)
- [x] Metadata-driven validation and DQ rule configuration
- [x] Severity-aware DQ execution (ERROR blocks, WARNING logs)
- [x] Validation and DQ results persistence for auditability
- [x] Silver transformation registry with per-entity transformation functions
- [x] Silver transformation loader (metadata-to-function resolution)
- [x] Silver schema and business rule validators

---

## Design Principles

| Principle | Application |
| --- | --- |
| Single Responsibility | Each module owns one concern: reading, writing, validation, state, auditing |
| Configuration over Code | New entities require metadata changes, not code changes |
| Separation of Concerns | Orchestration (engine) is decoupled from execution (reader, writer, validation) |
| Fail Fast, Fail Loud | Typed exceptions propagate immediately; no silent failures |
| Idempotency | Re-runs produce the same result; change detection prevents duplicate processing |
| Loose Coupling | Components communicate through function signatures, not shared state |
| Observability First | Every run is tracked end-to-end: pipeline_runs, audit_log, watermarks, validation_results, dq_results |
| Extensibility | Adding formats, strategies, rules, or layers requires no modifications to the engine |
| Schema Enforcement | Schema-on-read catches structural issues at ingestion, not downstream |
| Auto-Recovery | Dropped tables trigger automatic state reset and full re-ingestion |
| Defense in Depth | Retry for transient failures, validation for structural issues, DQ for semantic issues |
| Registry Pattern | Silver transformations and validators are plug-in components resolved at runtime |

---

## Tech Stack

| Technology | Role |
| --- | --- |
| Python / PySpark | Core processing language and distributed compute |
| Delta Lake | Storage format for ACID transactions, time travel, schema evolution |
| Databricks | Execution platform with Unity Catalog governance |
| Unity Catalog | Table registration, access control, lineage |
| Azure Data Lake Storage Gen2 | Underlying object storage (Raw, Bronze, Silver, Gold containers) |
| Databricks Photon | Vectorized query engine for accelerated Delta operations |

---

## Future Roadmap

| Phase | Enhancement | Description |
| --- | --- | --- |
| 2 | Silver Engine Integration | Wire silver_engine into end-to-end pipeline with Bronze output as input |
| 2 | Silver Validators | Implement schema conformance and business rule checks for Silver output |
| 2 | Gold Aggregations | Business KPIs, standings, and reporting tables |
| 3 | Streaming Ingestion | Auto Loader integration for near-real-time file arrival |
| 3 | CDC / Merge Engine | Change Data Capture with configurable merge strategies |
| 3 | Monitoring and Alerting | Operational dashboards, SLA tracking, failure notifications |
| 4 | Multi-Environment Promotion | Dev/staging/prod configuration with environment isolation |
| 4 | Orchestration | Lakeflow Jobs with dependency graphs and conditional execution |
| 4 | Declarative Pipeline Migration | Lakeflow Spark Declarative Pipelines for managed execution |

---

## Entities

The framework currently ingests the following Formula 1 data sources:

| Entity | Format | Load Type | Strategy | Partition |
| --- | --- | --- | --- | --- |
| circuits | CSV | full | -- | -- |
| constructors | JSON | full | -- | -- |
| drivers | JSON | full | -- | -- |
| races | CSV | full | -- | -- |
| results | JSON | incremental | file_tracking | race_year |
| sprints | JSON | incremental | file_tracking | race_year |

---

## Running the Framework

```python
# From the bronze_runner notebook:

import sys
sys.path.insert(0, "/Workspace/Users/<user>/project-f1")

from src.ingestion.bronze_ingestion_engine import run

# Ingest a single entity
run(spark, "circuits")

# Ingest all entities
entities = ["circuits", "constructors", "drivers", "races", "results", "sprints"]
for entity in entities:
    run(spark, entity)
```

The `run()` function is the sole entry point. It accepts a `SparkSession` and an `entity_name`, handles the full lifecycle internally, and returns the fully qualified table name on success or `None` if no data was processed.

---

## Conclusion

This project demonstrates that a well-architected ingestion framework eliminates the need for per-entity pipeline code. By externalizing configuration, enforcing schemas at read time, validating data quality through metadata-driven rules, tracking state at file granularity, and providing full observability through audit and run tables, the framework delivers production-grade reliability from a minimal codebase.

The current implementation covers Bronze ingestion with validation, data quality, and retry resilience, plus a Silver transformation layer with registry-based per-entity transformations. As Gold aggregations, streaming, and CDC layers are added, the same metadata-driven, component-oriented architecture scales without requiring structural changes to the core engine.
