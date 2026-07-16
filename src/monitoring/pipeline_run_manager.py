from datetime import datetime
import uuid

from pyspark.sql.functions import col, lit

from src.common.constants import (
    PIPELINE_RUNS_TABLE,
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED
)


def start_run(
    spark,
    pipeline_name: str,
    entity_name: str,
    created_by: str = "system",
) -> tuple[str, datetime]:
    """
    Create a new pipeline run with RUNNING status.
    Returns the run_id and start_time.
    """

    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    run_df = spark.createDataFrame(
        [
            (
                run_id,
                pipeline_name,
                entity_name,
                start_time,
                None,
                None,
                RUNNING,
                0,
                0,
                None,
                created_by,
            )
        ],
        schema="""
            run_id STRING,
            pipeline_name STRING,
            entity_name STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_ms BIGINT,
            status STRING,
            records_processed BIGINT,
            files_processed BIGINT,
            error_message STRING,
            created_by STRING
        """,
    )

    (
        run_df.write
        .mode("append")
        .saveAsTable(PIPELINE_RUNS_TABLE)
    )

    return run_id, start_time


# def update_metrics(
#     spark,
#     run_id: str,
#     records_processed: int = 0,
#     files_processed: int = 0,
# ) -> None:
#     """
#     Update pipeline metrics.
#     """

#     df = spark.table(PIPELINE_RUNS_TABLE)

#     (
#         df.withColumn(
#             "records_processed",
#             (
#                 col("records_processed")
#                 if col("run_id") != run_id
#                 else lit(records_processed)
#             ),
#         )
#     )

#     spark.sql(
#         f"""
#         UPDATE {PIPELINE_RUNS_TABLE}
#         SET
#             records_processed = {records_processed},
#             files_processed = {files_processed}
#         WHERE run_id = '{run_id}'
#         """
#     )


def complete_run(
    spark,
    run_id: str,
    start_time: datetime,
    status: str,
    records_processed: int,
    files_processed: int,
) -> None:
    ...
    """
    Mark pipeline execution as successful.
    """

    end_time = datetime.now()

    duration_ms = int(
        (end_time - start_time).total_seconds() * 1000
    )

    spark.sql(
        f"""
        UPDATE {PIPELINE_RUNS_TABLE}
        SET
            status = '{SUCCESS}',
            end_time = TIMESTAMP('{end_time}'),
            duration_ms = {duration_ms},
            records_processed = {records_processed},
            files_processed = {files_processed}
        WHERE run_id = '{run_id}'
        """
    )


def fail_run(
    spark,
    run_id: str,
    start_time: datetime,
    error_message: str,
) -> None:
    """
    Mark pipeline execution as failed.
    """

    end_time = datetime.now()

    duration_ms = int(
        (end_time - start_time).total_seconds() * 1000
    )

    error_message = error_message.replace("'", "''")

    spark.sql(
        f"""
        UPDATE {PIPELINE_RUNS_TABLE}
        SET
            status = '{FAILED}',
            end_time = TIMESTAMP('{end_time}'),
            duration_ms = {duration_ms},
            error_message = '{error_message}'
        WHERE run_id = '{run_id}'
        """
    )