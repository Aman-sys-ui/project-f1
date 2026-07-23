from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_races(df: DataFrame) -> DataFrame:

    return df.select(
        col("season").cast("int").alias("season"),
        col("round").cast("int").alias("round"),
        col("circuitId").alias("circuit_id"),
        col("raceName").alias("race_name"),
        col("date").cast("date").alias("race_date"),
        col("url"),
    )
