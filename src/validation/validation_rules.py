"""
Reusable validation rules for the ingestion framework.

Each validation rule returns a dictionary describing the validation result.
The rules never write to Delta tables and never update pipeline state.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
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
        rule_name="Schema Validation",
        rule_type="SCHEMA",
        severity="ERROR",
        passed=passed,
        failed_records = 0 if passed else 1,
        details="; ".join(details),
    )


def validate_required_columns(
    df: DataFrame,
    required_columns: list[str],
) -> ValidationResult:

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    passed = len(missing_columns) == 0

    return ValidationResult(
        rule_name="Required Columns Validation",
        rule_type="REQUIRED_COLUMN",
        severity="ERROR",
        passed=passed,
        failed_records = 0 if passed else 1,
        details=""
        if passed
        else f"Missing columns: {missing_columns}",
    )


def validate_not_null(
    df: DataFrame,
    column_name: str,
) -> ValidationResult:

    failed_records = (
        df
        .filter(col(column_name).isNull())
        .count()
    )

    passed = failed_records == 0

    return ValidationResult(
        rule_name=f"{column_name} Not Null Validation",
        rule_type="NOT_NULL",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else f"{failed_records} NULL values found in '{column_name}'.",
    )