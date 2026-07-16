"""Write DataFrame to Bronze layer."""

from pyspark.sql import DataFrame
from src.common.constants import (BRONZE_BASE_PATH, CATALOG_NAME)
from src.common.exceptions import WriteException
from src.common.retry import retry
from src.common.logger import get_logger

logger = get_logger(__name__)

@retry(retries=2 ,delay=10 ,retry_on=(WriteException),logger=logger)
def write_bronze(spark, df: DataFrame, target_schema: str, target_table: str, mode: str, partition_column:str | None = None) -> str:

    try:
        table_name = (
            f"{CATALOG_NAME}.{target_schema}.{target_table}"
        )

        table_path = (
            f"{BRONZE_BASE_PATH}/{target_table}"
        )

        writer = (
            df.write
            .format("delta")
            .mode(mode)
        )

        if partition_column:
           writer = writer.partitionBy(
                partition_column
            )
        writer.save(table_path)
        
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{table_path}'
            """
        )

        return table_name

    except Exception as exc:
        raise WriteException(f"Failed to write table '{table_name}'") from exc