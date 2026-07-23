from dataclasses import dataclass


@dataclass
class DQResult:
    """
    Represents the outcome of a Data Quality rule.
    """

    rule_name: str
    rule_type: str
    severity: str
    passed: bool
    failed_records: int
    details: str
