import pytest

from src.common.exceptions import ValidationException
from src.silver.validator import validate_silver_output
from src.silver.validators.business_validator import validate_non_negative
from src.silver.validators.schema_validator import validate_schema


class TestSchemaValidator:
    def test_passes_when_columns_match_exactly(self, spark):
        df = spark.createDataFrame([("1", "a")], ["circuit_id", "circuit_name"])

        result = validate_schema(df, expected_columns=["circuit_id", "circuit_name"])

        assert result.passed is True

    def test_fails_when_column_missing(self, spark):
        df = spark.createDataFrame([("1",)], ["circuit_id"])

        result = validate_schema(df, expected_columns=["circuit_id", "circuit_name"])

        assert result.passed is False
        assert "Missing columns" in result.details


class TestValidateNonNegative:
    def test_passes_when_all_values_non_negative(self, spark):
        df = spark.createDataFrame([(0,), (25,)], ["points"])

        result = validate_non_negative(df, column_name="points")

        assert result.passed is True

    def test_fails_and_counts_negative_values(self, spark):
        df = spark.createDataFrame([(10,), (-1,)], ["points"])

        result = validate_non_negative(df, column_name="points")

        assert result.passed is False
        assert result.failed_records == 1
        assert result.rule_type == "NON_NEGATIVE"


class TestValidateSilverOutput:
    def test_passes_for_well_formed_circuits_output(self, spark):
        df = spark.createDataFrame(
            [("1", "Silverstone", 52.07, -1.01, "Silverstone", "UK", "https://x")],
            [
                "circuit_id",
                "circuit_name",
                "latitude",
                "longitude",
                "locality",
                "country",
                "url",
            ],
        )

        validate_silver_output("circuits", df)  # should not raise

    def test_raises_when_schema_does_not_match(self, spark):
        df = spark.createDataFrame([("1",)], ["circuit_id"])

        with pytest.raises(ValidationException):
            validate_silver_output("circuits", df)

    def test_raises_when_primary_key_has_nulls(self, spark):
        df = spark.createDataFrame(
            [(None, "Mercedes", "German", "https://x")],
            "constructor_id STRING, name STRING, nationality STRING, url STRING",
        )

        with pytest.raises(ValidationException):
            validate_silver_output("constructors", df)

    def test_raises_when_negative_business_value_present(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "d1",
                    "c1",
                    2024,
                    1,
                    "Bahrain GP",
                    "2024-03-02",
                    1,
                    56,
                    44,
                    -5.0,
                    1,
                    "1",
                    "Finished",
                    "https://x",
                )
            ],
            [
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
            ],
        )

        with pytest.raises(ValidationException):
            validate_silver_output("results", df)

    def test_passes_through_unregistered_entity_without_schema_check(self, spark):
        """An entity with no EXPECTED_COLUMNS entry only runs whatever
        per-entity business rules exist (none, here) — it should not raise
        just because it isn't in the schema/business rule registries."""
        df = spark.createDataFrame([("1",)], ["some_column"])

        validate_silver_output("unregistered_entity", df)  # should not raise
