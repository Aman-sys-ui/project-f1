"""State manager for tracking processed files with change detection.

Detects file modifications using a triple check:
  1. source_file - new file never seen before
  2. last_modified_time + file_size - quick change detection
  3. content_hash (MD5 of content) - catches same-size replacements

Table: dbw_practice.control.ingestion_state
  entity_name, source_file, file_size, last_modified_time,
  content_hash, status, run_id, processed_at, processing_duration_ms
"""

from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5

from src.common.constants import CATALOG_NAME, CONTROL_SCHEMA
from src.common.exceptions import StateException

INGESTION_STATE_TABLE = (
    f"{CATALOG_NAME}.{CONTROL_SCHEMA}.ingestion_state"
)


# State Retrieval

def get_processed_files(
    spark: SparkSession,
    entity_name: str
) -> Dict[str, Dict]:
    """Return dict keyed by source_file."""
    try:
        rows = (
            spark.table(INGESTION_STATE_TABLE)
            .filter(f"entity_name = '{entity_name}'")
            .select(
                "source_file",
                "last_modified_time",
                "file_size",
                "content_hash"
            )
            .collect()
        )
    except Exception as e:
        raise StateException(
            f"Failed to retrieve processed files for "
            f"entity '{entity_name}': {e}"
        ) from e

    return {
        row.source_file: {
            "last_modified_time": row.last_modified_time,
            "file_size": row.file_size,
            "content_hash": row.content_hash
        }
        for row in rows
    }


# Change Detection

def file_needs_processing(
    source_file: str,
    mod_time,
    file_size: int,
    content_hash: str,
    processed_files: Dict[str, Dict]
) -> bool:
    """Determine if a file needs (re)processing."""
    if source_file not in processed_files:
        return True

    prev = processed_files[source_file]

    if prev["last_modified_time"] != mod_time:
        return True

    if prev["file_size"] != file_size:
        return True

    if prev["content_hash"] != content_hash:
        return True

    return False


# Source File Metadata
def get_source_file_info(
    spark: SparkSession,
    source_path: str
) -> List[Dict]:
    """Single binaryFile read: path + mod_time + size + hash."""
    try:
        rows = (
            spark.read.format("binaryFile")
            .load(source_path)
            .select(
                col("path").alias("source_file"),
                col("modificationTime").alias(
                    "last_modified_time"
                ),
                col("length").alias("file_size"),
                md5(col("content")).alias("content_hash")
            )
            .collect()
        )
    except Exception as e:
        raise StateException(
            f"Failed to read source file metadata from "
            f"'{source_path}': {e}"
        ) from e

    return [row.asDict() for row in rows]


# State Mutation

def mark_files_processed(
    spark: SparkSession,
    entity_name: str,
    file_records: List[Dict],
    run_id: str,
    processing_duration_ms: int = None
) -> None:
    """Record processed files with full metadata."""
    if not file_records:
        return

    now = datetime.utcnow()
    records = [
        (
            entity_name,
            r["source_file"],
            r["file_size"],
            r["last_modified_time"],
            r["content_hash"],
            "SUCCESS",
            run_id,
            now,
            processing_duration_ms
        )
        for r in file_records
    ]

    try:
        df = spark.createDataFrame(
            records,
            schema=[
                "entity_name",
                "source_file",
                "file_size",
                "last_modified_time",
                "content_hash",
                "status",
                "run_id",
                "processed_at",
                "processing_duration_ms"
            ]
        )

        df.createOrReplaceTempView("_new_state")
        spark.sql(f"""
            MERGE INTO {INGESTION_STATE_TABLE} AS tgt
            USING _new_state AS src
            ON tgt.entity_name = src.entity_name
               AND tgt.source_file = src.source_file
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except StateException:
        raise
    except Exception as e:
        raise StateException(
            f"Failed to mark files as processed for "
            f"entity '{entity_name}': {e}"
        ) from e


# State Management

def reset_state(
    spark: SparkSession,
    entity_name: str
) -> None:
    """Clear all processed file records for an entity."""
    try:
        spark.sql(
            f"DELETE FROM {INGESTION_STATE_TABLE} "
            f"WHERE entity_name = '{entity_name}'"
        )
    except Exception as e:
        raise StateException(
            f"Failed to reset state for "
            f"entity '{entity_name}': {e}"
        ) from e


def target_table_exists(
    spark: SparkSession,
    target_schema: str,
    target_table: str
) -> bool:
    """Check if the target bronze table exists."""
    table_name = (
        f"{CATALOG_NAME}.{target_schema}.{target_table}"
    )
    try:
        return spark.catalog.tableExists(table_name)
    except Exception as e:
        raise StateException(
            f"Failed to check existence of table "
            f"'{table_name}': {e}"
        ) from e
