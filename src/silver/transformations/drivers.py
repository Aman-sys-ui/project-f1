from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def transform_drivers(df: DataFrame) -> DataFrame:

    return df.select(
    col("driverId").alias("driver_id"),
    col("name.givenName").alias("firstname"),
    col("name.familyName").alias("surname"),
    col("dateOfBirth").cast("date").alias("date_of_birth"),
    col("nationality"),
    col("url"),
)