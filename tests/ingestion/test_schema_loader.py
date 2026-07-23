from pyspark.sql.types import StructType

from src.ingestion.schema_loader import get_schema


def test_known_schema_returns_struct_type():
    schema = get_schema("circuits_schema")
    assert isinstance(schema, StructType)
    assert "circuitId" in schema.fieldNames()


def test_drivers_schema_has_nested_name_struct():
    schema = get_schema("drivers_schema")
    name_field = schema["name"]
    assert isinstance(name_field.dataType, StructType)
    assert "givenName" in name_field.dataType.fieldNames()


def test_unknown_schema_returns_none():
    assert get_schema("does_not_exist") is None
