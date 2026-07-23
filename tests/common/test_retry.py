import pytest

from src.common.retry import retry


class FlakyError(Exception):
    pass


class OtherError(Exception):
    pass


def test_returns_result_on_first_success():
    calls = []

    @retry(retries=3, delay=0, retry_on=(FlakyError,))
    def always_ok():
        calls.append(1)
        return "ok"

    assert always_ok() == "ok"
    assert len(calls) == 1


def test_succeeds_after_transient_failures():
    calls = []

    @retry(retries=3, delay=0, retry_on=(FlakyError,))
    def fails_twice_then_ok():
        calls.append(1)
        if len(calls) < 3:
            raise FlakyError("transient")
        return "ok"

    assert fails_twice_then_ok() == "ok"
    assert len(calls) == 3


def test_raises_after_exhausting_retries():
    calls = []

    @retry(retries=3, delay=0, retry_on=(FlakyError,))
    def always_fails():
        calls.append(1)
        raise FlakyError("permanent")

    with pytest.raises(FlakyError):
        always_fails()

    assert len(calls) == 3


def test_non_matching_exception_propagates_immediately():
    calls = []

    @retry(retries=3, delay=0, retry_on=(FlakyError,))
    def raises_other():
        calls.append(1)
        raise OtherError("not retryable")

    with pytest.raises(OtherError):
        raises_other()

    assert len(calls) == 1


def test_logs_each_retry_attempt(mocker):
    logger = mocker.Mock()
    calls = []

    @retry(retries=2, delay=0, retry_on=(FlakyError,), logger=logger)
    def fails_once_then_ok():
        calls.append(1)
        if len(calls) < 2:
            raise FlakyError("transient")
        return "ok"

    fails_once_then_ok()

    logger.warning.assert_called_once()
