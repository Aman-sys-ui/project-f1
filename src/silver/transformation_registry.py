"""
Registry of Silver transformation functions.

Maps transformation names from metadata to the corresponding
Python transformation functions.
"""

from src.silver.transformations.drivers import transform_drivers
from src.silver.transformations.constructors import transform_constructors
from src.silver.transformations.circuits import transform_circuits
from src.silver.transformations.races import transform_races
from src.silver.transformations.results import transform_results
from src.silver.transformations.sprints import transform_sprints


TRANSFORMATION_REGISTRY = {
    "transform_drivers": transform_drivers,
    "transform_constructors": transform_constructors,
    "transform_circuits": transform_circuits,
    "transform_races": transform_races,
    "transform_results": transform_results,
    "transform_sprints": transform_sprints,
}
