from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def transform_constructors(df: DataFrame) -> DataFrame:

    return df.select(
        col("constructorId").alias("constructor_id"),
        col("name"),
        col("nationality"),
        col("url"),
    )
