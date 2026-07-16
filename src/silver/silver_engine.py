import logging

from pyspark.sql import DataFrame

from src.common.config_loader import get_silver_config
from src.common.constants import CATALOG_NAME
from src.common.metadata import add_silver_metadata
from src.silver.transformation_registry import TRANSFORMATION_REGISTRY


logger = logging.getLogger(__name__)


class SilverEngine:
    def __init__(self, spark):
        self.spark = spark

    def _load_config(self, entity_name: str) -> dict:
        return get_silver_config(self.spark, entity_name)

    def _read_bronze(self, source_schema: str, source_table: str) -> DataFrame:
        table_name = f"{CATALOG_NAME}.{source_schema}.{source_table}"

        logger.info("Reading Bronze table: %s", table_name)

        return self.spark.table(table_name)

    def _write_silver(
        self,
        df: DataFrame,
        target_schema: str,
        target_table: str,
        write_mode: str,
    ) -> None:
        table_name = f"{CATALOG_NAME}.{target_schema}.{target_table}"

        logger.info("Writing Silver table: %s", table_name)

        (
            df.write
            .format("delta")
            .mode(write_mode)
            .saveAsTable(table_name)
        )

    def run(
        self,
        entity_name: str,
        pipeline_run_id: str,
    ) -> DataFrame:
        try:
            logger.info(
                "Starting Silver pipeline for entity: %s",
                entity_name,
            )

            config = self._load_config(entity_name)

            bronze_df = self._read_bronze(
                config["source_schema"],
                config["source_table"],
            )

            transformation_name = config["transformation_name"]

            logger.info(
                "Executing transformation: %s",
                transformation_name,
            )

            transformation = TRANSFORMATION_REGISTRY[
                transformation_name
            ]

            silver_df = transformation(bronze_df)

            silver_df = add_silver_metadata(
                silver_df,
                pipeline_run_id,
            )

            self._write_silver(
                df=silver_df,
                target_schema=config["target_schema"],
                target_table=config["target_table"],
                write_mode=config["write_mode"],
            )

            logger.info(
                "Successfully completed Silver pipeline for entity: %s",
                entity_name,
            )

            return silver_df

        except Exception:
            logger.exception(
                "Silver pipeline failed for entity: %s",
                entity_name,
            )
            raise