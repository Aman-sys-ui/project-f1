import uuid
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    year,
)

from src.common.config_loader import get_entity_config
from src.common.constants import (
    FULL_LOAD,
    INCREMENTAL_LOAD,
)
from src.common.logger import get_logger
from src.ingestion.state_manager import (
    file_needs_processing,
    get_processed_files,
    get_source_file_info,
    mark_files_processed,
    reset_state,
    target_table_exists,
)
from src.validation.validation_runner import run_validation
from src.ingestion.bronze_writer import write_bronze
from src.ingestion.reader import read_source
from src.ingestion.schema_loader import get_schema
from src.monitoring.audit_logger import (
    log_audit,
    update_watermark,
)
from src.monitoring.pipeline_run_manager import (
    complete_run,
    fail_run,
    start_run,
)
from src.common.exceptions import (
    ConfigurationException,
    PipelineException,
    ValidationException,
)

logger = get_logger(__name__)


def add_metadata_columns(
    df: DataFrame,
    run_id: str,
) -> DataFrame:
    return df.withColumns(
        {
            "_ingestion_ts": current_timestamp(),
            "_source_file": col("_metadata.file_path"),
            "_pipeline_run_id": lit(run_id),
        }
    )


def run(spark, entity_name: str,):
    
    run_id, start_time = start_run(
        spark=spark,
        pipeline_name="bronze_ingestion",
        entity_name=entity_name,
    )

    record_count = 0
    files_processed = 0

    try:
        # 1. Read metadata
        config = get_entity_config(
            spark,
            entity_name,
        )

        logger.info(
            f"Processing entity: {entity_name}"
        )

        # 2. Load schema
        schema = get_schema(
            config["schema_file"]
        )

        # 3. Determine write mode
        load_type = config["load_type"]

        if load_type == FULL_LOAD:
            mode = "overwrite"

        elif load_type == INCREMENTAL_LOAD:
            mode = "append"

        else:
            raise ConfigurationException(
                f"Unsupported load type: {load_type}"
            )

        # 4. Change detection
        incremental_strategy = config.get(
            "incremental_strategy"
        )

        files_to_process = None
        file_info = None

        if incremental_strategy == "file_tracking":

            if not target_table_exists(
                spark,
                config["target_schema"],
                config["target_table"],
            ):
                reset_state(
                    spark,
                    entity_name,
                )

                logger.info(
                    "Target table missing. Resetting ingestion state."
                )

                processed_state = {}

            else:
                processed_state = get_processed_files(
                    spark,
                    entity_name,
                )

            logger.info(
                f"State contains {len(processed_state)} tracked files."
            )

            file_info = get_source_file_info(
                spark,
                config["source_path"],
            )

            files_to_process = {
                file["source_file"]
                for file in file_info
                if file_needs_processing(
                    file["source_file"],
                    file["last_modified_time"],
                    file["file_size"],
                    file["content_hash"],
                    processed_state,
                )
            }

            logger.info(
                f"{len(files_to_process)} files require processing."
            )

            if not files_to_process:

                logger.info(
                    "No new or modified files found."
                )

                complete_run(
                    spark=spark,
                    run_id=run_id,
                    start_time=start_time,
                    status="SKIPPED",
                    records_processed=0,
                    files_processed=0,
                )

                return None

        # 5. Read Source
        df = read_source(
            spark,
            config["source_path"],
            config["source_format"],
            schema,
            files_to_process,
            multiline=config.get("multiline") or False,
        )

        # 6. Validation
        validation_passed = run_validation(
            spark=spark,
            run_id=run_id,
            entity_name=entity_name,
            df=df,
        )

        if not validation_passed:
            raise ValidationException(
                f"Validation failed for entity '{entity_name}'."
            )

        # 7. Partition Column
        partition_column = config.get(
            "partition_column"
        )

        if partition_column:
            df = df.withColumn(
                partition_column,
                year(col("date")),
            )

        # 8. Metadata columns
        df = add_metadata_columns(
            df,
            run_id,
        )

        # 9. Count
        df = df.cache()

        record_count = df.count()

        if record_count == 0:

            logger.info(
                "No records to write."
            )

            complete_run(
                spark=spark,
                run_id=run_id,
                start_time=start_time,
                status="SKIPPED",
                records_processed=0,
                files_processed=0,
            )

            return None

        # 10. Write Bronze
        table_name = write_bronze(
            spark,
            df,
            config["target_schema"],
            config["target_table"],
            mode,
            partition_column,
        )

        # 11. Update ingestion state
        if incremental_strategy == "file_tracking":

            file_records = [
                file
                for file in file_info
                if file["source_file"] in files_to_process
            ]

            files_processed = len(file_records)

            elapsed_ms = int(
                (datetime.now() - start_time).total_seconds() * 1000
            )

            mark_files_processed(
                spark,
                entity_name,
                file_records,
                run_id,
                processing_duration_ms=elapsed_ms,
            )

            logger.info(
                f"Marked {files_processed} files as processed."
            )

        # 12. Audit
        log_audit(
            spark,
            run_id,
            entity_name,
            record_count,
            "SUCCESS",
        )

        # 13. Watermark
        if (
            incremental_strategy == "file_tracking"
            and file_info
            and files_to_process
        ):
            max_mod_time = max(
                file["last_modified_time"]
                for file in file_info
                if file["source_file"] in files_to_process
            )

            update_watermark(
                spark,
                entity_name,
                max_mod_time,
            )

        # 14. Complete pipeline run
        complete_run(
            spark=spark,
            run_id=run_id,
            start_time=start_time,
            status="SUCCESS",
            records_processed=record_count,
            files_processed=files_processed,
        )

        return table_name

    except PipelineException as exc:

            logger.exception(str(exc))

            fail_run(
                spark=spark,
                run_id=run_id,
                start_time=start_time,
                error_message=str(exc),
            )

            raise

    except Exception as exc:

            logger.exception("Unexpected pipeline failure.")

            fail_run(
                spark=spark,
                run_id=run_id,
                start_time=start_time,
                error_message=str(exc),
            )

            raise