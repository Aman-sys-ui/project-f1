from dataclasses import dataclass


@dataclass
class ValidationResult:
    """
    Represents the outcome of a validation rule.
    """

    rule_name: str
    rule_type: str
    severity: str
    passed: bool
    failed_records: int
    details: str