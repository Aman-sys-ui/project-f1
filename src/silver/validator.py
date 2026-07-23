"""Silver-level validation coordinator."""

from pyspark.sql import DataFrame

from src.common.exceptions import ValidationException
from src.silver.validators.business_validator import (
    validate_non_negative,
    validate_not_null,
)
from src.silver.validators.schema_validator import validate_schema

# Expected output columns per entity, keyed by the entity_name used in
# silver_entity_config. Kept here (rather than a metadata table, unlike
# Bronze's validation_rules) since Silver has a small, fixed set of
# hand-written transformations rather than a dynamic rule set.
EXPECTED_COLUMNS = {
    "circuits": [
        "circuit_id",
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "url",
    ],
    "constructors": ["constructor_id", "name", "nationality", "url"],
    "drivers": [
        "driver_id",
        "firstname",
        "surname",
        "date_of_birth",
        "nationality",
        "url",
    ],
    "races": ["season", "round", "circuit_id", "race_name", "race_date", "url"],
    "results": [
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
    "sprints": [
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
}

NOT_NULL_COLUMNS = {
    "circuits": ["circuit_id"],
    "constructors": ["constructor_id"],
    "drivers": ["driver_id"],
    "races": ["circuit_id"],
    "results": ["driver_id", "constructor_id"],
    "sprints": ["driver_id", "constructor_id"],
}

NON_NEGATIVE_COLUMNS = {
    "results": ["grid", "points"],
    "sprints": ["grid", "points"],
}


def validate_silver_output(entity_name: str, df: DataFrame) -> None:
    """
    Validate a Silver-layer DataFrame before it is written.

    Raises ValidationException if any rule fails.
    """

    results = []

    expected_columns = EXPECTED_COLUMNS.get(entity_name)
    if expected_columns is not None:
        results.append(validate_schema(df, expected_columns))

    for column_name in NOT_NULL_COLUMNS.get(entity_name, []):
        results.append(validate_not_null(df, column_name))

    for column_name in NON_NEGATIVE_COLUMNS.get(entity_name, []):
        results.append(validate_non_negative(df, column_name))

    failures = [result for result in results if not result.passed]

    if failures:
        details = "; ".join(
            result.details for result in failures if result.details
        )
        raise ValidationException(
            f"Silver validation failed for entity '{entity_name}': {details}"
        )
