import glob
import os
import sys

import pytest

# On Windows there is usually no "python3" on PATH (only python.exe), but
# Spark's executor-side worker processes hardcode that name unless told
# otherwise — without this, any action that spawns a Python worker (e.g. a
# shuffle) fails with "CreateProcess error=2, The system cannot find the
# file specified".
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# PySpark 3.4.1 is pinned to match Databricks Runtime 13.3 LTS, which
# predates JDK 21's stronger module encapsulation (Spark's native memory
# code calls a java.nio.DirectByteBuffer constructor JDK 21 removed
# outright — no --add-opens flag can bring it back). The local test driver
# JVM must run on JDK 17 instead. This only affects the JVM PySpark spawns
# for this test session; it does not touch the machine's JAVA_HOME/PATH.
def _find_local_jdk17():
    candidates = sorted(
        glob.glob(r"C:\Program Files\Eclipse Adoptium\jdk-17*")
    )
    return candidates[0] if candidates else None

_jdk17_home = _find_local_jdk17()
if _jdk17_home:
    os.environ["JAVA_HOME"] = _jdk17_home
    os.environ["PATH"] = os.path.join(_jdk17_home, "bin") + os.pathsep + os.environ["PATH"]

from pyspark.sql import SparkSession  # noqa: E402  (must follow the JAVA_HOME override above)


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for unit tests.

    No Delta / Unity Catalog required: tests that hit control tables
    mock the relevant spark calls instead of writing real Delta tables.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("project-f1-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
