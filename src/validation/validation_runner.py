from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import DataFrame

from src.common.constants import VALIDATION_RESULTS_TABLE
from src.common.models import ValidationResult
from src.validation.rule_loader import load_validation_rules
from src.validation.validation_engine import ValidationEngine
from src.validation.validation_rules import (
    validate_not_null,
    validate_required_columns,
    validate_schema,
)


def run_validation(
    spark,
    run_id: str,
    entity_name: str,
    df: DataFrame,
) -> bool:
    """
    Execute metadata-driven validation rules for an entity.

    Returns
    -------
    bool
        True if all validations passed.
    """

    engine = ValidationEngine()

    rules = load_validation_rules(
        spark,
        entity_name,
    )

    for rule in rules:

        rule_type = rule["rule_type"]

        if rule_type == "SCHEMA":

            engine.execute(
                validate_schema,
                df,
                df.columns,
            )

        elif rule_type == "REQUIRED_COLUMN":

            engine.execute(
                validate_required_columns,
                df,
                [rule["column_name"]],
            )

        elif rule_type == "NOT_NULL":

            engine.execute(
                validate_not_null,
                df,
                rule["column_name"],
            )

    results = engine.get_results()

    if not results:
        return True

    validation_df = spark.createDataFrame(
        [
            Row(
                run_id=run_id,
                entity_name=entity_name,
                rule_name=result.rule_name,
                rule_type=result.rule_type,
                severity=result.severity,
                passed=result.passed,
                failed_records=result.failed_records,
                execution_time=datetime.now(),
                details=result.details,
            )
            for result in results
        ]
    )

    (
        validation_df.write
        .mode("append")
        .saveAsTable(VALIDATION_RESULTS_TABLE)
    )

    return all(
        result.passed
        for result, rule in zip(results, rules)
        if rule["severity"] == "ERROR"
    )