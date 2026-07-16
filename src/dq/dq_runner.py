from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import Row

from src.common.constants import DQ_RESULTS_TABLE
from src.dq.dq_engine import DQEngine
from src.dq.dq_rule_loader import load_dq_rules
from src.dq.dq_rules import (
    validate_duplicate_keys,
    validate_duplicate_rows,
)


def run_dq(
    spark,
    run_id: str,
    entity_name: str,
    df: DataFrame,
) -> bool:
    """
    Execute metadata-driven Data Quality rules
    for an entity.
    """

    engine = DQEngine()

    rules = load_dq_rules(
        spark,
        entity_name,
    )

    for rule in rules:

        rule_type = rule["rule_type"]

        if rule_type == "DUPLICATE_KEY":

            engine.execute(
                validate_duplicate_keys,
                df,
                rule["column_name"],
            )

        elif rule_type == "DUPLICATE_ROW":

            engine.execute(
                validate_duplicate_rows,
                df,
            )

    results = engine.get_results()

    dq_df = spark.createDataFrame(
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
        dq_df.write
        .mode("append")
        .saveAsTable(DQ_RESULTS_TABLE)
    )

    return engine.passed()