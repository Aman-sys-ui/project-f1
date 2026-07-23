import pytest

import src.silver.silver_engine as engine_module
from src.common.exceptions import ValidationException
from src.silver.silver_engine import SilverEngine


def silver_config(**overrides):
    config = {
        "source_schema": "bronze",
        "source_table": "circuits",
        "transformation_name": "fake_transform",
        "target_schema": "silver",
        "target_table": "circuits",
        "write_mode": "overwrite",
    }
    config.update(overrides)
    return config


@pytest.fixture
def patched(mocker):
    fake_transform = mocker.Mock(side_effect=lambda df: df)

    return {
        "get_silver_config": mocker.patch.object(engine_module, "get_silver_config"),
        "transformation_registry": mocker.patch.object(
            engine_module, "TRANSFORMATION_REGISTRY", {"fake_transform": fake_transform}
        ),
        "fake_transform": fake_transform,
        "validate_silver_output": mocker.patch.object(engine_module, "validate_silver_output"),
        "add_silver_metadata": mocker.patch.object(
            engine_module, "add_silver_metadata", side_effect=lambda df, run_id: df
        ),
    }


class TestSilverEngineRun:
    def test_success_path_transforms_validates_and_writes(self, mocker, patched):
        patched["get_silver_config"].return_value = silver_config()
        engine = SilverEngine(spark=object())
        bronze_df = object()
        mocker.patch.object(engine, "_read_bronze", return_value=bronze_df)
        write_mock = mocker.patch.object(engine, "_write_silver")

        result = engine.run(entity_name="circuits", pipeline_run_id="run-1")

        assert result is bronze_df
        patched["fake_transform"].assert_called_once_with(bronze_df)
        patched["validate_silver_output"].assert_called_once_with("circuits", bronze_df)
        write_mock.assert_called_once_with(
            df=bronze_df,
            target_schema="silver",
            target_table="circuits",
            write_mode="overwrite",
        )

    def test_validation_failure_prevents_write(self, mocker, patched):
        patched["get_silver_config"].return_value = silver_config()
        patched["validate_silver_output"].side_effect = ValidationException("bad output")
        engine = SilverEngine(spark=object())
        mocker.patch.object(engine, "_read_bronze", return_value=object())
        write_mock = mocker.patch.object(engine, "_write_silver")

        with pytest.raises(ValidationException):
            engine.run(entity_name="circuits", pipeline_run_id="run-1")

        write_mock.assert_not_called()

    def test_unknown_transformation_raises_key_error(self, mocker, patched):
        patched["get_silver_config"].return_value = silver_config(
            transformation_name="does_not_exist"
        )
        engine = SilverEngine(spark=object())
        mocker.patch.object(engine, "_read_bronze", return_value=object())
        write_mock = mocker.patch.object(engine, "_write_silver")

        with pytest.raises(KeyError):
            engine.run(entity_name="circuits", pipeline_run_id="run-1")

        write_mock.assert_not_called()
