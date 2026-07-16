"""
Loads metadata-driven Data Quality rules.
"""

from src.common.constants import DQ_RULES_TABLE


def load_dq_rules(
    spark,
    entity_name: str,
) -> list[dict]:
    """
    Load all enabled Data Quality rules
    for an entity.
    """

    rows = (
        spark.table(DQ_RULES_TABLE)
        .filter(
            f"""
            entity_name = '{entity_name}'
            AND enabled = true
            """
        )
        .collect()
    )

    return [
        {
            "rule_name": row.rule_name,
            "rule_type": row.rule_type,
            "column_name": row.column_name,
            "rule_value": row.rule_value,
            "threshold": row.threshold,
            "severity": row.severity,
        }
        for row in rows
    ]