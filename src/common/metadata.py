"""
Common metadata utilities.

Provides reusable helper functions for adding
pipeline metadata across Bronze, Silver, and Gold.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

def add_silver_metadata(df: DataFrame, pipeline_run_id: str) -> DataFrame:
    """
    Add standard Silver metadata columns.

    Parameters
    ----------
    df : DataFrame
        Input Silver DataFrame.

    pipeline_run_id : str
        Current pipeline execution ID.

    Returns
    -------
    DataFrame
        DataFrame with Silver metadata columns.
    """
    return (
        df.withColumns(
            {
                "_processed_ts": current_timestamp(),
                "_pipeline_run_id": lit(pipeline_run_id),
            }
        )
    )




# One small improvement

# Since your Bronze layer already generates a pipeline_run_id, I'd recommend passing the entire pipeline context rather than individual values in the future:

# add_silver_metadata(
#     df,
#     pipeline_context,
# )

# where pipeline_context could later include:

# pipeline_run_id
# pipeline_name
# environment
# batch_id
# trigger_type