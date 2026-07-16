# Catalog
CATALOG_NAME = "dbw_practice01"

# Schemas
CONTROL_SCHEMA = "control"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Metadata Tables
ENTITY_CONFIG_TABLE = (
    f"{CATALOG_NAME}.{CONTROL_SCHEMA}.entity_config"
)

PIPELINE_RUNS_TABLE = (
    f"{CATALOG_NAME}.{CONTROL_SCHEMA}.pipeline_runs"
)

AUDIT_LOG_TABLE = (
    f"{CATALOG_NAME}.{CONTROL_SCHEMA}.audit_log"
)

WATERMARK_TABLE = (
    f"{CATALOG_NAME}.{CONTROL_SCHEMA}.watermarks"
)

VALIDATION_RESULTS_TABLE = (
    f"{CATALOG_NAME}.control.validation_results"
)

VALIDATION_RULES_TABLE = (
    f"{CATALOG_NAME}.control.validation_rules"
)

DQ_RULES_TABLE = (
    f"{CATALOG_NAME}.control.dq_rules"
)

DQ_RESULTS_TABLE = (
    f"{CATALOG_NAME}.control.dq_results"
)

# Load Type
FULL_LOAD = "full"
INCREMENTAL_LOAD = "incremental"

# File Formats
CSV = "csv"
JSON = "json"

# Audit Columns
INGESTION_TS_COL = "_ingestion_ts"
SOURCE_FILE_COL = "_source_file"
PIPELINE_RUN_ID_COL = "_pipeline_run_id"


# ADLS PATH
BRONZE_BASE_PATH = (
    "abfss://bronze@devstorageaccountpc01.dfs.core.windows.net"
)

SILVER_BASE_PATH = (
    "abfss://silver@devstorageaccountpc01.dfs.core.windows.net"
)

GOLD_BASE_PATH = (
    "abfss://gold@devstorageaccountpc01.dfs.core.windows.net"
)

RUNNING = "RUNNING"
SUCCESS = "SUCCESS"
FAILED = "FAILED"
SKIPPED = "SKIPPED"
