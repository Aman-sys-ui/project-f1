from src.common.models import ValidationResult
from src.validation.validation_engine import ValidationEngine


def make_result(passed):
    return ValidationResult(
        rule_name="fake rule",
        rule_type="FAKE",
        severity="ERROR",
        passed=passed,
        failed_records=0 if passed else 1,
        details="",
    )


class TestValidationEngine:
    def test_execute_stores_and_returns_result(self):
        engine = ValidationEngine()
        rule = lambda: make_result(True)

        result = engine.execute(rule)

        assert result.passed is True
        assert engine.get_results() == [result]

    def test_passed_is_true_when_no_rules_ran(self):
        engine = ValidationEngine()
        assert engine.passed() is True

    def test_passed_is_true_when_all_rules_pass(self):
        engine = ValidationEngine()
        engine.execute(lambda: make_result(True))
        engine.execute(lambda: make_result(True))

        assert engine.passed() is True

    def test_passed_is_false_when_any_rule_fails(self):
        engine = ValidationEngine()
        engine.execute(lambda: make_result(True))
        engine.execute(lambda: make_result(False))

        assert engine.passed() is False

    def test_execute_passes_args_and_kwargs_through_to_rule(self):
        engine = ValidationEngine()
        captured = {}

        def rule(a, b, keyword=None):
            captured["a"] = a
            captured["b"] = b
            captured["keyword"] = keyword
            return make_result(True)

        engine.execute(rule, 1, 2, keyword="x")

        assert captured == {"a": 1, "b": 2, "keyword": "x"}
