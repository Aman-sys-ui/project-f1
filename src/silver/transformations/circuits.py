from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_circuits(df: DataFrame) -> DataFrame:

    return df.select(
        col("circuitId").alias("circuit_id"),
        col("circuitName").alias("circuit_name"),
        col("lat").cast("double").alias("latitude"),
        col("long").cast("double").alias("longitude"),
        col("locality"),
        col("country"),
        col("url"),
    )
