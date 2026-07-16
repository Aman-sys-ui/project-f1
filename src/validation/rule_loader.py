from pyspark.sql import Row

from src.common.constants import VALIDATION_RULES_TABLE


def load_validation_rules(spark, entity_name: str,) -> list[dict]:
    """
    Load enabled validation rules for an entity.
    """

    rows = (
        spark.table(VALIDATION_RULES_TABLE)
        .filter(
            f"entity_name = '{entity_name}' "
            "AND enabled = true"
        )
        .collect()
    )

    return [
        {
            "rule_name": row.rule_name,
            "rule_type": row.rule_type,
            "column_name": row.column_name,
            "rule_value": row.rule_value,
            "severity": row.severity,
        }
        for row in rows
    ]