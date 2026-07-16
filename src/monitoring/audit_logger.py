from datetime import datetime

from pyspark.sql import Row

from src.common.constants import (
    AUDIT_LOG_TABLE,
    WATERMARK_TABLE,
)
from src.common.retry import retry
from src.common.logger import get_logger
from src.common.exceptions import AuditException

logger = get_logger(__name__)

@retry(retries=2, delay=2, retry_on=(AuditException,), logger=logger)
def log_audit(
    spark,
    run_id: str,
    entity_name: str,
    record_count: int,
    status: str,
) -> None:
    """
    Write an audit record for an ingestion run.
    """
    try:
        audit_df = spark.createDataFrame(
            [
                Row(
                    run_id=run_id,
                    entity_name=entity_name,
                    record_count=record_count,
                    status=status,
                    created_ts=datetime.now(),
                )
            ]
        )

        (
            audit_df.write
            .mode("append")
            .saveAsTable(AUDIT_LOG_TABLE)
        )
    except Exception as exc:
        raise AuditException(
            f"Failed Audit '{entity_name}'."
        ) from exc


def update_watermark(
    spark,
    entity_name: str,
    watermark_value,
) -> None:
    """
    Upsert the latest processed watermark for an entity.
    """

    watermark_df = spark.createDataFrame(
        [
            Row(
                entity_name=entity_name,
                watermark_value=str(watermark_value),
                updated_ts=datetime.now(),
            )
        ]
    )

    watermark_df.createOrReplaceTempView(
        "_watermark_update"
    )

    spark.sql(
        f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING _watermark_update AS source
        ON target.entity_name = source.entity_name

        WHEN MATCHED THEN
            UPDATE SET *

        WHEN NOT MATCHED THEN
            INSERT *
        """
    )