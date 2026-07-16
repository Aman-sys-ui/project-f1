from pyspark.sql import DataFrame

from src.common.models import ValidationResult
from src.validation.validation_rules import (
    validate_not_null,
    validate_required_columns,
    validate_schema,
)


class ValidationEngine:
    """
    Executes validation rules against a DataFrame.
    """

    def __init__(self):
        self.results: list[ValidationResult] = []

    def execute(self, rule, *args, **kwargs, ) -> ValidationResult:

        result = rule(
            *args,
            **kwargs
        )

        self.results.append(result)

        return result

    def passed(self) -> bool:

        return all(
            result.passed
            for result in self.results
        )

    def get_results(self) -> list[ValidationResult]:

        return self.results