from src.silver.transformation_registry import TRANSFORMATION_REGISTRY


def get_transformation(name: str):

    if name not in TRANSFORMATION_REGISTRY:
        raise ValueError(
            f"Transformation '{name}' not registered."
        )

    return TRANSFORMATION_REGISTRY[name]