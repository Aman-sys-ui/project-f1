from datetime import datetime

import pytest

import src.ingestion.bronze_ingestion_engine as engine
from src.common.exceptions import ConfigurationException, ValidationException


def make_fake_df(record_count):
    """A minimal chainable stand-in for a PySpark DataFrame."""

    class FakeDF:
        def cache(self):
            return self

        def count(self):
            return record_count

        def withColumns(self, *_args, **_kwargs):
            return self

        def withColumn(self, *_args, **_kwargs):
            return self

    return FakeDF()


@pytest.fixture
def patched(mocker):
    """Patch every collaborator bronze_ingestion_engine.run() delegates to."""

    return {
        "start_run": mocker.patch.object(
            engine, "start_run", return_value=("run-1", datetime(2026, 1, 1))
        ),
        "get_entity_config": mocker.patch.object(engine, "get_entity_config"),
        "get_schema": mocker.patch.object(engine, "get_schema", return_value=object()),
        "target_table_exists": mocker.patch.object(engine, "target_table_exists"),
        "reset_state": mocker.patch.object(engine, "reset_state"),
        "get_processed_files": mocker.patch.object(engine, "get_processed_files", return_value={}),
        "get_source_file_info": mocker.patch.object(engine, "get_source_file_info", return_value=[]),
        "file_needs_processing": mocker.patch.object(engine, "file_needs_processing"),
        "read_source": mocker.patch.object(engine, "read_source"),
        "run_validation": mocker.patch.object(engine, "run_validation", return_value=True),
        "write_bronze": mocker.patch.object(
            engine, "write_bronze", return_value="dbw_practice01.bronze.circuits"
        ),
        "mark_files_processed": mocker.patch.object(engine, "mark_files_processed"),
        "log_audit": mocker.patch.object(engine, "log_audit"),
        "update_watermark": mocker.patch.object(engine, "update_watermark"),
        "complete_run": mocker.patch.object(engine, "complete_run"),
        "fail_run": mocker.patch.object(engine, "fail_run"),
    }


def full_load_config(**overrides):
    config = {
        "schema_file": "circuits_schema",
        "load_type": "full",
        "target_schema": "bronze",
        "target_table": "circuits",
        "source_path": "abfss://raw/circuits",
        "source_format": "csv",
        "partition_column": None,
        "incremental_strategy": None,
    }
    config.update(overrides)
    return config


class TestFullLoadSuccess:
    def test_writes_with_overwrite_mode_and_returns_table_name(self, patched):
        patched["get_entity_config"].return_value = full_load_config()
        patched["read_source"].return_value = make_fake_df(record_count=5)

        result = engine.run(spark=object(), entity_name="circuits")

        assert result == "dbw_practice01.bronze.circuits"
        assert patched["write_bronze"].call_args[0][4] == "overwrite"

    def test_records_success_with_record_count(self, patched):
        patched["get_entity_config"].return_value = full_load_config()
        patched["read_source"].return_value = make_fake_df(record_count=5)

        engine.run(spark=object(), entity_name="circuits")

        patched["complete_run"].assert_called_once()
        call_kwargs = patched["complete_run"].call_args.kwargs
        assert call_kwargs["status"] == "SUCCESS"
        assert call_kwargs["records_processed"] == 5

    def test_does_not_touch_incremental_state_or_watermark(self, patched):
        patched["get_entity_config"].return_value = full_load_config()
        patched["read_source"].return_value = make_fake_df(record_count=5)

        engine.run(spark=object(), entity_name="circuits")

        patched["target_table_exists"].assert_not_called()
        patched["mark_files_processed"].assert_not_called()
        patched["update_watermark"].assert_not_called()
        patched["log_audit"].assert_called_once()


class TestIncrementalNoNewFiles:
    def test_skips_and_returns_none_without_reading_or_writing(self, patched):
        patched["get_entity_config"].return_value = full_load_config(
            load_type="incremental",
            incremental_strategy="file_tracking",
        )
        patched["target_table_exists"].return_value = True
        patched["get_source_file_info"].return_value = [
            {
                "source_file": "a.json",
                "last_modified_time": "2024-01-01",
                "file_size": 10,
                "content_hash": "h1",
            }
        ]
        patched["file_needs_processing"].return_value = False

        result = engine.run(spark=object(), entity_name="results")

        assert result is None
        patched["read_source"].assert_not_called()
        patched["write_bronze"].assert_not_called()
        call_kwargs = patched["complete_run"].call_args.kwargs
        assert call_kwargs["status"] == "SKIPPED"
        assert call_kwargs["records_processed"] == 0

    def test_resets_state_when_target_table_missing(self, patched):
        patched["get_entity_config"].return_value = full_load_config(
            load_type="incremental",
            incremental_strategy="file_tracking",
        )
        patched["target_table_exists"].return_value = False
        patched["file_needs_processing"].return_value = False

        engine.run(spark=object(), entity_name="results")

        patched["reset_state"].assert_called_once()
        patched["get_processed_files"].assert_not_called()


class TestZeroRecordsAfterRead:
    def test_skips_write_and_returns_none(self, patched):
        patched["get_entity_config"].return_value = full_load_config()
        patched["read_source"].return_value = make_fake_df(record_count=0)

        result = engine.run(spark=object(), entity_name="circuits")

        assert result is None
        patched["write_bronze"].assert_not_called()
        call_kwargs = patched["complete_run"].call_args.kwargs
        assert call_kwargs["status"] == "SKIPPED"


class TestValidationFailure:
    def test_raises_validation_exception_and_fails_run(self, patched):
        patched["get_entity_config"].return_value = full_load_config()
        patched["read_source"].return_value = make_fake_df(record_count=5)
        patched["run_validation"].return_value = False

        with pytest.raises(ValidationException):
            engine.run(spark=object(), entity_name="circuits")

        patched["fail_run"].assert_called_once()
        patched["complete_run"].assert_not_called()
        patched["write_bronze"].assert_not_called()


class TestUnsupportedLoadType:
    def test_raises_configuration_exception_and_fails_run(self, patched):
        patched["get_entity_config"].return_value = full_load_config(load_type="streaming")

        with pytest.raises(ConfigurationException):
            engine.run(spark=object(), entity_name="circuits")

        patched["fail_run"].assert_called_once()
