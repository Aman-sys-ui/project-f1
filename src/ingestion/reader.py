from typing import Optional, Set

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.constants import CSV, JSON
from src.common.retry import retry
from src.common.logger import get_logger
from src.common.exceptions import ReadException

logger = get_logger(__name__)

@retry(retries=3,delay=5,retry_on=(ReadException,),logger=logger,)
def read_source(spark, source_path: str, source_format: str, schema, files_to_include: Optional[Set[str]] = None, multiline: bool = False) -> DataFrame:
    """Read source data, optionally limited to specific files.

    Args:
        files_to_include: If provided, only rows from
            these file paths are returned. If None,
            all files are read (full load behavior).
    """
    try:
        reader = spark.read.schema(schema)

        if source_format == CSV:
            df = reader.option("header", True)\
                .csv(source_path)

        elif source_format == JSON:
            df = (
                reader.option("multiline", multiline)
                .json(source_path)
            )

        else:
            raise ValueError(f"Unsupported format: {source_format}")

        # Filter to only include specified files
        if files_to_include is not None:
            df = df.filter(
                col("_metadata.file_path")
                .isin(list(files_to_include))
            )

        return df

    except Exception as exc:
            raise ReadException(f"Failed to read source {source_path}'.") from exc