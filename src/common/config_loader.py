from pyspark.sql import SparkSession
from src.common.exceptions import ConfigurationException
from src.common.constants import ENTITY_CONFIG_TABLE

def get_entity_config(spark, entity_name: str):
    try:
        row =  (
            spark.table(ENTITY_CONFIG_TABLE)
            .filter(f"entity_name = '{entity_name}'")
            .first()
        )

        return row.asDict() if row else None
    except Exception as exc:
        raise ConfigurationException(f"Failed getting config '{entity_name}'.") from exc


def get_silver_config(spark, entity_name):
    return (
        spark.table("dbw_practice01.control.silver_entity_config")
             .filter(f"entity_name = '{entity_name}'")
             .first()
             .asDict()
    )
