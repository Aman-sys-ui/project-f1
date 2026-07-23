from src.validation.validation_rules import (
    validate_not_null,
    validate_required_columns,
    validate_schema,
)


class TestValidateSchema:
    def test_passes_when_columns_match_exactly(self, spark):
        df = spark.createDataFrame([("1", "a")], ["id", "name"])

        result = validate_schema(df, expected_columns=["id", "name"])

        assert result.passed is True
        assert result.rule_type == "SCHEMA"
        assert result.failed_records == 0

    def test_fails_when_column_missing(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])

        result = validate_schema(df, expected_columns=["id", "name"])

        assert result.passed is False
        assert "Missing columns" in result.details
        assert result.failed_records == 1

    def test_fails_when_unexpected_column_present(self, spark):
        df = spark.createDataFrame([("1", "a")], ["id", "extra"])

        result = validate_schema(df, expected_columns=["id"])

        assert result.passed is False
        assert "Unexpected columns" in result.details


class TestValidateRequiredColumns:
    def test_passes_when_all_required_columns_present(self, spark):
        df = spark.createDataFrame([("1", "a")], ["id", "name"])

        result = validate_required_columns(df, required_columns=["id"])

        assert result.passed is True
        assert result.rule_type == "REQUIRED_COLUMN"

    def test_fails_when_required_column_missing(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])

        result = validate_required_columns(df, required_columns=["id", "name"])

        assert result.passed is False
        assert "name" in result.details


class TestValidateNotNull:
    def test_passes_when_no_nulls(self, spark):
        df = spark.createDataFrame([("1",), ("2",)], ["id"])

        result = validate_not_null(df, column_name="id")

        assert result.passed is True
        assert result.failed_records == 0

    def test_fails_and_counts_null_records(self, spark):
        df = spark.createDataFrame(
            [("1",), (None,), (None,)],
            ["id"],
        )

        result = validate_not_null(df, column_name="id")

        assert result.passed is False
        assert result.failed_records == 2
        assert result.rule_type == "NOT_NULL"
