"""Business rule validation for silver output."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.models import ValidationResult
from src.validation.validation_rules import validate_not_null

__all__ = ["validate_not_null", "validate_non_negative"]


def validate_non_negative(
    df: DataFrame,
    column_name: str,
) -> ValidationResult:

    failed_records = (
        df.filter(col(column_name) < 0)
        .count()
    )

    passed = failed_records == 0

    return ValidationResult(
        rule_name=f"{column_name} Non-Negative Validation",
        rule_type="NON_NEGATIVE",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) have a negative "
            f"value in '{column_name}'."
        ),
    )
