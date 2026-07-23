from datetime import date

from src.ingestion.schema_loader import (
    circuits_schema,
    constructors_schema,
    drivers_schema,
    races_schema,
    results_schema,
    sprints_schema,
)
from src.silver.transformations.circuits import transform_circuits
from src.silver.transformations.constructors import transform_constructors
from src.silver.transformations.drivers import transform_drivers
from src.silver.transformations.races import transform_races
from src.silver.transformations.results import transform_results
from src.silver.transformations.sprints import transform_sprints


class TestTransformCircuits:
    def test_renames_and_types_columns(self, spark):
        df = spark.createDataFrame(
            [("1", "https://x", "Silverstone", "52.07", "-1.01", "Silverstone", "UK")],
            schema=circuits_schema,
        )

        result = transform_circuits(df)

        assert set(result.columns) == {
            "circuit_id",
            "circuit_name",
            "latitude",
            "longitude",
            "locality",
            "country",
            "url",
        }
        row = result.first()
        assert row["circuit_id"] == "1"
        assert row["latitude"] == 52.07
        assert row["longitude"] == -1.01


class TestTransformConstructors:
    def test_renames_columns(self, spark):
        df = spark.createDataFrame(
            [("1", "Mercedes", "German", "https://x")],
            schema=constructors_schema,
        )

        result = transform_constructors(df)

        assert set(result.columns) == {"constructor_id", "name", "nationality", "url"}
        assert result.first()["constructor_id"] == "1"


class TestTransformDrivers:
    def test_renames_and_flattens_nested_name(self, spark):
        df = spark.createDataFrame(
            [("1990-01-07", "hamilton", ("Hamilton", "Lewis"), "British", "https://x")],
            schema=drivers_schema,
        )

        result = transform_drivers(df)

        assert set(result.columns) == {
            "driver_id",
            "firstname",
            "surname",
            "date_of_birth",
            "nationality",
            "url",
        }
        row = result.first()
        assert row["firstname"] == "Lewis"
        assert row["surname"] == "Hamilton"
        assert row["date_of_birth"] == date(1990, 1, 7)


class TestTransformRaces:
    def test_renames_and_types_columns(self, spark):
        df = spark.createDataFrame(
            [("2024", "1", "https://x", "Bahrain GP", "2024-03-02", "3")],
            schema=races_schema,
        )

        result = transform_races(df)

        assert set(result.columns) == {
            "season",
            "round",
            "circuit_id",
            "race_name",
            "race_date",
            "url",
        }
        row = result.first()
        assert row["season"] == 2024
        assert row["round"] == 1
        assert row["race_date"] == date(2024, 3, 2)


class TestTransformResults:
    def test_renames_columns_and_keeps_typed_fields(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "c1",
                    "2024-03-02",
                    "d1",
                    1,
                    56,
                    44,
                    25.0,
                    1,
                    "1",
                    "Bahrain GP",
                    1,
                    2024,
                    "Finished",
                    "https://x",
                )
            ],
            schema=results_schema,
        )

        result = transform_results(df)

        assert set(result.columns) == {
            "driver_id",
            "constructor_id",
            "season",
            "round",
            "race_name",
            "race_date",
            "grid",
            "laps",
            "number",
            "points",
            "position",
            "position_text",
            "status",
            "url",
        }
        row = result.first()
        assert row["driver_id"] == "d1"
        assert row["constructor_id"] == "c1"
        assert row["position_text"] == "1"
        assert row["race_date"] == date(2024, 3, 2)


class TestTransformSprints:
    def test_same_shape_as_results(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "c1",
                    "2024-03-01",
                    "d1",
                    1,
                    17,
                    44,
                    8.0,
                    1,
                    "1",
                    "Bahrain GP Sprint",
                    1,
                    2024,
                    "Finished",
                    "https://x",
                )
            ],
            schema=sprints_schema,
        )

        result = transform_sprints(df)

        assert set(result.columns) == {
            "driver_id",
            "constructor_id",
            "season",
            "round",
            "race_name",
            "race_date",
            "grid",
            "laps",
            "number",
            "points",
            "position",
            "position_text",
            "status",
            "url",
        }
