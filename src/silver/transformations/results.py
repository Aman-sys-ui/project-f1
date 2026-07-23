from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_results(df: DataFrame) -> DataFrame:

    return df.select(
        col("driverId").alias("driver_id"),
        col("constructorId").alias("constructor_id"),
        col("season"),
        col("round"),
        col("raceName").alias("race_name"),
        col("date").cast("date").alias("race_date"),
        col("grid"),
        col("laps"),
        col("number"),
        col("points"),
        col("position"),
        col("positionText").alias("position_text"),
        col("status"),
        col("url"),
    )
