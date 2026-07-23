import pytest

from src.common.utils import (
    build_fqn,
    format_duration,
    replace_null_marker,
    safe_strip,
    to_snake_case,
    truncate_message,
)


class TestToSnakeCase:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("CircuitId", "circuit_id"),
            ("driverId", "driver_id"),
            ("Race Name", "race_name"),
            ("already_snake", "already_snake"),
        ],
    )
    def test_converts_to_snake_case(self, value, expected):
        assert to_snake_case(value) == expected

    def test_consecutive_capitals_are_not_split(self):
        """Known limitation: the regex only splits lower/digit -> upper
        transitions, so runs of capitals (acronyms) collapse together.
        """
        assert to_snake_case("HTTPResponse") == "httpresponse"


class TestSafeStrip:
    def test_strips_whitespace(self):
        assert safe_strip("  hello  ") == "hello"

    def test_returns_none_for_blank_string(self):
        assert safe_strip("   ") is None

    def test_returns_none_for_none_input(self):
        assert safe_strip(None) is None

    def test_leaves_clean_string_unchanged(self):
        assert safe_strip("clean") == "clean"


class TestReplaceNullMarker:
    def test_replaces_default_ergast_marker(self):
        assert replace_null_marker("\\N") is None

    def test_leaves_other_values_unchanged(self):
        assert replace_null_marker("Hamilton") == "Hamilton"

    def test_custom_marker(self):
        assert replace_null_marker("NULL", marker="NULL") is None


class TestBuildFqn:
    def test_builds_fully_qualified_name(self):
        assert build_fqn("cat", "schema", "table") == "cat.schema.table"


class TestFormatDuration:
    def test_seconds_only(self):
        assert format_duration(45.9) == "45s"

    def test_minutes_and_seconds(self):
        assert format_duration(83.4) == "1m 23s"

    def test_zero(self):
        assert format_duration(0) == "0s"


class TestTruncateMessage:
    def test_short_message_unchanged(self):
        assert truncate_message("hello") == "hello"

    def test_long_message_truncated_with_suffix(self):
        message = "x" * 600
        result = truncate_message(message, max_length=500)
        assert len(result) == 500
        assert result.endswith("...")

    def test_message_exactly_at_limit_unchanged(self):
        message = "x" * 500
        assert truncate_message(message, max_length=500) == message
