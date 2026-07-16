"""
Reusable Data Quality rules.

Each rule evaluates a specific aspect of data quality and returns
a DQResult object. Rules do not write to Delta tables or manage
pipeline execution.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.dq_models import DQResult

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.dq_models import DQResult


def validate_duplicate_keys(
    df: DataFrame,
    column_name: str,
) -> DQResult:
    """
    Validate that a business key is unique.

    failed_records represents the number of rows
    participating in duplicate keys.
    """

    duplicate_keys = (
        df.groupBy(column_name)
        .count()
        .filter(col("count") > 1)
        .select(column_name)
    )

    failed_records = (
        df.join(
            duplicate_keys,
            on=column_name,
            how="inner",
        )
        .count()
    )

    passed = failed_records == 0

    return DQResult(
        rule_name=f"{column_name} Duplicate Key Validation",
        rule_type="DUPLICATE_KEY",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) belong to duplicate "
            f"business keys in '{column_name}'."
        ),
    )


def validate_duplicate_rows(
    df: DataFrame,
) -> DQResult:
    """
    Validate that the dataset does not contain duplicate rows.
    """

    total_records = df.count()

    distinct_records = df.distinct().count()

    failed_records = total_records - distinct_records

    passed = failed_records == 0

    return DQResult(
        rule_name="Duplicate Row Validation",
        rule_type="DUPLICATE_ROW",
        severity="WARNING",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"Found {failed_records} duplicate row(s)."
        ),
    )

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.dq_models import DQResult


def validate_allowed_values(
    df: DataFrame,
    column_name: str,
    rule_value: str,
) -> DQResult:
    """
    Validate that a column contains only allowed values.

    rule_value example:
        "M,F,U"
    """

    allowed_values = [
        value.strip()
        for value in rule_value.split(",")
    ]

    failed_records = (
        df.filter(
            ~col(column_name).isin(allowed_values)
        ).count()
    )

    passed = failed_records == 0

    return DQResult(
        rule_name=f"{column_name} Allowed Values Validation",
        rule_type="ALLOWED_VALUES",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) contain invalid values "
            f"for '{column_name}'."
        ),
    )

from pyspark.sql.functions import col


def validate_regex(
    df: DataFrame,
    column_name: str,
    rule_value: str,
) -> DQResult:
    """
    Validate values using a regular expression.

    Example regex:

    ^[A-Z]{3}$
    """

    failed_records = (
        df.filter(
            ~col(column_name).rlike(rule_value)
        ).count()
    )

    passed = failed_records == 0

    return DQResult(
        rule_name=f"{column_name} Regex Validation",
        rule_type="REGEX",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) failed regex validation."
        ),
    )

from pyspark.sql.functions import col


def validate_numeric_range(
    df: DataFrame,
    column_name: str,
    rule_value: str,
) -> DQResult:
    """
    Validate numeric values fall within an inclusive range.

    rule_value example:

        "0,100"
    """

    min_value, max_value = map(
        float,
        rule_value.split(",")
    )

    failed_records = (
        df.filter(
            (col(column_name) < min_value)
            | (col(column_name) > max_value)
        ).count()
    )

    passed = failed_records == 0

    return DQResult(
        rule_name=f"{column_name} Numeric Range Validation",
        rule_type="NUMERIC_RANGE",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) are outside "
            f"[{min_value}, {max_value}]."
        ),
    )

from pyspark.sql.functions import col, lit, to_date


def validate_date_range(
    df: DataFrame,
    column_name: str,
    rule_value: str,
) -> DQResult:
    """
    Validate dates lie within an inclusive range.

    rule_value example:

        "1950-01-01,2050-12-31"
    """

    min_date, max_date = [
        value.strip()
        for value in rule_value.split(",")
    ]

    failed_records = (
        df.filter(
            (to_date(col(column_name)) < lit(min_date))
            | (to_date(col(column_name)) > lit(max_date))
        ).count()
    )

    passed = failed_records == 0

    return DQResult(
        rule_name=f"{column_name} Date Range Validation",
        rule_type="DATE_RANGE",
        severity="ERROR",
        passed=passed,
        failed_records=failed_records,
        details=""
        if passed
        else (
            f"{failed_records} record(s) are outside "
            f"{min_date} - {max_date}."
        ),
    )


from pyspark.sql.functions import col
def validate_null_percentage(
    df: DataFrame,
    column_name: str,
    threshold: float,
) -> DQResult:
    """
    Validate that NULL percentage does not exceed the threshold.

    threshold example:

        5.0
    """

    total_records = df.count()

    null_records = (
        df.filter(
            col(column_name).isNull()
        ).count()
    )

    null_percentage = (
        0.0
        if total_records == 0
        else (null_records / total_records) * 100
    )

    passed = null_percentage <= threshold

    return DQResult(
        rule_name=f"{column_name} Null Percentage Validation",
        rule_type="NULL_PERCENTAGE",
        severity="WARNING",
        passed=passed,
        failed_records=null_records,
        details=""
        if passed
        else (
            f"NULL percentage {null_percentage:.2f}% "
            f"exceeds threshold {threshold:.2f}%."
        ),
    )