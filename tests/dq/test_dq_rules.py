from src.dq.dq_rules import (
    validate_allowed_values,
    validate_date_range,
    validate_duplicate_keys,
    validate_duplicate_rows,
    validate_null_percentage,
    validate_numeric_range,
    validate_regex,
)


class TestValidateDuplicateKeys:
    def test_passes_when_key_is_unique(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

        result = validate_duplicate_keys(df, column_name="id")

        assert result.passed is True
        assert result.rule_type == "DUPLICATE_KEY"

    def test_fails_and_counts_rows_sharing_a_duplicate_key(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (1, "b"), (2, "c")],
            ["id", "name"],
        )

        result = validate_duplicate_keys(df, column_name="id")

        assert result.passed is False
        assert result.failed_records == 2


class TestValidateDuplicateRows:
    def test_passes_when_no_duplicate_rows(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

        result = validate_duplicate_rows(df)

        assert result.passed is True
        assert result.severity == "WARNING"

    def test_fails_and_counts_duplicate_rows(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (1, "a"), (2, "b")],
            ["id", "name"],
        )

        result = validate_duplicate_rows(df)

        assert result.passed is False
        assert result.failed_records == 1


class TestValidateAllowedValues:
    def test_passes_when_all_values_allowed(self, spark):
        df = spark.createDataFrame([("M",), ("F",)], ["gender"])

        result = validate_allowed_values(df, column_name="gender", rule_value="M,F,U")

        assert result.passed is True

    def test_fails_for_disallowed_value(self, spark):
        df = spark.createDataFrame([("M",), ("X",)], ["gender"])

        result = validate_allowed_values(df, column_name="gender", rule_value="M,F,U")

        assert result.passed is False
        assert result.failed_records == 1


class TestValidateRegex:
    def test_passes_when_values_match_pattern(self, spark):
        df = spark.createDataFrame([("ABC",), ("XYZ",)], ["code"])

        result = validate_regex(df, column_name="code", rule_value="^[A-Z]{3}$")

        assert result.passed is True

    def test_fails_when_value_does_not_match(self, spark):
        df = spark.createDataFrame([("ABC",), ("ab1",)], ["code"])

        result = validate_regex(df, column_name="code", rule_value="^[A-Z]{3}$")

        assert result.passed is False
        assert result.failed_records == 1


class TestValidateNumericRange:
    def test_passes_when_within_range(self, spark):
        df = spark.createDataFrame([(10.0,), (50.0,)], ["points"])

        result = validate_numeric_range(df, column_name="points", rule_value="0,100")

        assert result.passed is True

    def test_fails_when_outside_range(self, spark):
        df = spark.createDataFrame([(10.0,), (150.0,)], ["points"])

        result = validate_numeric_range(df, column_name="points", rule_value="0,100")

        assert result.passed is False
        assert result.failed_records == 1


class TestValidateDateRange:
    def test_passes_when_within_range(self, spark):
        df = spark.createDataFrame([("2020-05-01",)], ["race_date"])

        result = validate_date_range(
            df, column_name="race_date", rule_value="1950-01-01,2050-12-31"
        )

        assert result.passed is True

    def test_fails_when_outside_range(self, spark):
        df = spark.createDataFrame([("1900-01-01",)], ["race_date"])

        result = validate_date_range(
            df, column_name="race_date", rule_value="1950-01-01,2050-12-31"
        )

        assert result.passed is False
        assert result.failed_records == 1


class TestValidateNullPercentage:
    def test_passes_when_under_threshold(self, spark):
        df = spark.createDataFrame([("a",), ("b",), (None,)], ["value"])

        result = validate_null_percentage(df, column_name="value", threshold=50.0)

        assert result.passed is True

    def test_fails_when_over_threshold(self, spark):
        df = spark.createDataFrame([("a",), (None,), (None,)], ["value"])

        result = validate_null_percentage(df, column_name="value", threshold=10.0)

        assert result.passed is False
        assert "exceeds threshold" in result.details
        assert result.severity == "WARNING"

    def test_handles_empty_dataframe_without_dividing_by_zero(self, spark):
        df = spark.createDataFrame([], "value STRING")

        result = validate_null_percentage(df, column_name="value", threshold=10.0)

        assert result.passed is True
