from src.common.dq_models import DQResult
from src.dq.dq_engine import DQEngine


def make_result(passed, severity):
    return DQResult(
        rule_name="fake rule",
        rule_type="FAKE",
        severity=severity,
        passed=passed,
        failed_records=0 if passed else 1,
        details="",
    )


class TestDQEngine:
    def test_passed_true_when_no_rules_ran(self):
        engine = DQEngine()
        assert engine.passed() is True

    def test_passed_true_when_all_error_rules_pass(self):
        engine = DQEngine()
        engine.execute(lambda: make_result(True, "ERROR"))

        assert engine.passed() is True

    def test_passed_false_when_an_error_rule_fails(self):
        engine = DQEngine()
        engine.execute(lambda: make_result(False, "ERROR"))

        assert engine.passed() is False

    def test_warning_failures_do_not_fail_the_run(self):
        """A failing WARNING-severity rule must not block the pipeline."""
        engine = DQEngine()
        engine.execute(lambda: make_result(True, "ERROR"))
        engine.execute(lambda: make_result(False, "WARNING"))

        assert engine.passed() is True
        assert len(engine.get_results()) == 2

    def test_get_results_returns_all_executed_results_in_order(self):
        engine = DQEngine()
        first = engine.execute(lambda: make_result(True, "ERROR"))
        second = engine.execute(lambda: make_result(False, "WARNING"))

        assert engine.get_results() == [first, second]
