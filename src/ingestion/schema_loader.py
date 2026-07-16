"""Schema definitions for raw F1 data sources."""


from pyspark.sql.types import *
from src.common.exceptions import SchemaException

circuits_schema = StructType([
    StructField("circuitId", StringType(), True),
    StructField("url", StringType(), True),
    StructField("circuitName", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country", StringType(), True)
])

constructors_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

drivers_schema = StructType([
    StructField("dateOfBirth", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField(
        "name",
        StructType([
            StructField("familyName", StringType(), True),
            StructField("givenName", StringType(), True)
        ]),
        True
    ),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

races_schema = StructType([
    StructField("season", StringType(), True),
    StructField("round", StringType(), True),
    StructField("url", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("date", StringType(), True),
    StructField("circuitId", StringType(), True)
])

results_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("date", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("grid", LongType(), True),
    StructField("laps", LongType(), True),
    StructField("number", LongType(), True),
    StructField("points", DoubleType(), True),
    StructField("position", LongType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("round", LongType(), True),
    StructField("season", LongType(), True),
    StructField("status", StringType(), True),
    StructField("url", StringType(), True)
])

sprints_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("date", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("grid", LongType(), True),
    StructField("laps", LongType(), True),
    StructField("number", LongType(), True),
    StructField("points", DoubleType(), True),
    StructField("position", LongType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("round", LongType(), True),
    StructField("season", LongType(), True),
    StructField("status", StringType(), True),
    StructField("url", StringType(), True)
])

SCHEMAS = {
    "circuits_schema": circuits_schema,
    "constructors_schema": constructors_schema,
    "drivers_schema": drivers_schema,
    "races_schema": races_schema,
    "results_schema": results_schema,
    "sprints_schema": sprints_schema
}


def get_schema(schema_name: str):
    try:
        return SCHEMAS.get(schema_name)
    except Exception as exc:
        raise SchemaException(f"Unable to load schema '{schema_name}'.") from exc






    