"""
Generic Data Quality Engine.

Responsible for executing DQ rules and collecting results.
The engine is rule-agnostic and does not know anything about
specific rule implementations.
"""

from src.common.dq_models import DQResult


class DQEngine:
    """
    Executes Data Quality rules and stores their results.
    """

    def __init__(self) -> None:
        self.results: list[DQResult] = []

    def execute(
        self,
        rule,
        *args,
        **kwargs,
    ) -> DQResult:
        """
        Execute a DQ rule and store its result.
        """

        result = rule(
            *args,
            **kwargs,
        )

        self.results.append(result)

        return result

    def passed(self) -> bool:
        """
        Returns True if all ERROR severity rules passed.

        WARNING failures do not fail the DQ execution.
        """

        return all(
            result.passed
            for result in self.results
            if result.severity == "ERROR"
        )

    def get_results(self) -> list[DQResult]:
        """
        Returns all DQ rule execution results.
        """

        return self.results