"""
Shared utility functions for the F1 Lakehouse Platform.

Design decisions:
    - Only genuinely reusable, stateless helpers live here.
    - Functions that logically belong to a specific layer stay in that layer.
    - No Spark imports — this module must work in unit tests without a
      SparkSession.

Functions:
    get_utc_now()       — Return current UTC datetime (no tzinfo).
    to_snake_case()     — Convert a string to snake_case.
    safe_strip()        — Strip whitespace from a string; return None if empty.
    replace_null_marker() — Replace the Ergast \\N null marker with None.
    build_fqn()         — Build a fully qualified UC table name.
    format_duration()   — Format elapsed seconds as a human-readable string.
    truncate_message()  — Truncate a string to a maximum length.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Time utilities
# ---------------------------------------------------------------------------


def get_utc_now() -> datetime:
    """Return the current UTC datetime without timezone info.

    We strip tzinfo so that the value is compatible with Delta Lake timestamp
    columns that expect naive datetimes (Spark TIMESTAMP type without TZ).

    Returns:
        Current UTC datetime as a naive ``datetime`` object.
    """
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def format_duration(seconds: float) -> str:
    """Format an elapsed duration in seconds as a human-readable string.

    Args:
        seconds: Elapsed time in seconds.

    Returns:
        A string like ``"1m 23s"`` or ``"45s"`` or ``"0s"``.

    Examples:
        >>> format_duration(83.4)
        '1m 23s'
        >>> format_duration(45.9)
        '45s'
    """
    total = int(seconds)
    minutes, secs = divmod(total, 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


# ---------------------------------------------------------------------------
# String utilities
# ---------------------------------------------------------------------------


def to_snake_case(value: str) -> str:
    """Convert a string to snake_case.

    Handles camelCase, PascalCase, spaces, and hyphens.

    Args:
        value: Input string.

    Returns:
        Snake_case version of the input.

    Examples:
        >>> to_snake_case("CircuitId")
        'circuit_id'
        >>> to_snake_case("driverId")
        'driver_id'
        >>> to_snake_case("Race Name")
        'race_name'
    """
    # Insert underscore before uppercase letters that follow lowercase or digits
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    # Replace spaces and hyphens with underscores
    s = re.sub(r"[\s\-]+", "_", s)
    return s.lower()


def safe_strip(value: Optional[str]) -> Optional[str]:
    """Strip leading/trailing whitespace from a string.

    Returns ``None`` when the result is an empty string.

    Args:
        value: Input string or ``None``.

    Returns:
        Stripped string, or ``None`` if the result is empty or input is None.

    Examples:
        >>> safe_strip("  hello  ")
        'hello'
        >>> safe_strip("   ")
        None  # (returns None, not shown as string)
        >>> safe_strip(None)
        None  # (returns None)
    """
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def replace_null_marker(value: Optional[str], marker: str = r"\N") -> Optional[str]:
    r"""Replace the Ergast CSV null marker (``\N``) with ``None``.

    The Ergast Formula 1 dataset uses ``\N`` to represent NULL values in CSV
    exports.  This must be converted to Python ``None`` before any type cast
    to avoid cast failures.

    Args:
        value:  Input string from a CSV cell.
        marker: The null marker to replace.  Defaults to ``\\N``.

    Returns:
        ``None`` if the value equals the marker; otherwise the original value.

    Examples:
        >>> replace_null_marker(r"\N")
        None  # (returns None)
        >>> replace_null_marker("Hamilton")
        'Hamilton'
    """
    if value == marker:
        return None
    return value


def truncate_message(message: str, max_length: int = 500) -> str:
    """Truncate a message string to a maximum character length.

    Used when writing error messages to Delta tables where column length
    is limited.

    Args:
        message:    Input message.
        max_length: Maximum allowed length (default: 500).

    Returns:
        Truncated string with ``"..."`` suffix if truncated.
    """
    if len(message) <= max_length:
        return message
    return message[: max_length - 3] + "..."


# ---------------------------------------------------------------------------
# UC table name utilities
# ---------------------------------------------------------------------------


def build_fqn(catalog: str, schema: str, table: str) -> str:
    """Build a fully qualified Unity Catalog table name.

    Args:
        catalog: Catalog name.
        schema:  Schema (database) name.
        table:   Table name.

    Returns:
        ``catalog.schema.table`` string.
    """
    return f"{catalog}.{schema}.{table}"
