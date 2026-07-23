"""Silver schema conformance checks."""

from pyspark.sql import DataFrame

from src.common.models import ValidationResult


def validate_schema(
    df: DataFrame,
    expected_columns: list[str],
) -> ValidationResult:

    actual_columns = df.columns

    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    unexpected_columns = [
        column
        for column in actual_columns
        if column not in expected_columns
    ]

    passed = (
        len(missing_columns) == 0
        and len(unexpected_columns) == 0
    )

    details = []

    if missing_columns:
        details.append(
            f"Missing columns: {missing_columns}"
        )

    if unexpected_columns:
        details.append(
            f"Unexpected columns: {unexpected_columns}"
        )

    return ValidationResult(
        rule_name="Silver Schema Validation",
        rule_type="SCHEMA",
        severity="ERROR",
        passed=passed,
        failed_records=0 if passed else 1,
        details="; ".join(details),
    )
