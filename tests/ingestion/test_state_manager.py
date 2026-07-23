from types import SimpleNamespace

import pytest

from src.common.exceptions import StateException
from src.ingestion.state_manager import (
    file_needs_processing,
    get_processed_files,
    mark_files_processed,
    reset_state,
    target_table_exists,
)


class TestFileNeedsProcessing:
    def test_new_file_needs_processing(self):
        assert file_needs_processing("a.csv", "2024-01-01", 100, "hash1", {}) is True

    def test_unchanged_file_does_not_need_processing(self):
        processed = {
            "a.csv": {
                "last_modified_time": "2024-01-01",
                "file_size": 100,
                "content_hash": "hash1",
            }
        }
        assert (
            file_needs_processing("a.csv", "2024-01-01", 100, "hash1", processed)
            is False
        )

    def test_changed_mod_time_needs_processing(self):
        processed = {
            "a.csv": {
                "last_modified_time": "2024-01-01",
                "file_size": 100,
                "content_hash": "hash1",
            }
        }
        assert (
            file_needs_processing("a.csv", "2024-01-02", 100, "hash1", processed)
            is True
        )

    def test_changed_size_needs_processing(self):
        processed = {
            "a.csv": {
                "last_modified_time": "2024-01-01",
                "file_size": 100,
                "content_hash": "hash1",
            }
        }
        assert (
            file_needs_processing("a.csv", "2024-01-01", 200, "hash1", processed)
            is True
        )

    def test_changed_content_hash_needs_processing(self):
        """Same size + mtime but different content (e.g. same-size edit)."""
        processed = {
            "a.csv": {
                "last_modified_time": "2024-01-01",
                "file_size": 100,
                "content_hash": "hash1",
            }
        }
        assert (
            file_needs_processing("a.csv", "2024-01-01", 100, "hash2", processed)
            is True
        )


class TestGetProcessedFiles:
    def test_returns_dict_keyed_by_source_file(self, mocker):
        rows = [
            SimpleNamespace(
                source_file="a.csv",
                last_modified_time="2024-01-01",
                file_size=100,
                content_hash="hash1",
            )
        ]
        spark = mocker.Mock()
        spark.table.return_value.filter.return_value.select.return_value.collect.return_value = rows

        result = get_processed_files(spark, "circuits")

        assert result == {
            "a.csv": {
                "last_modified_time": "2024-01-01",
                "file_size": 100,
                "content_hash": "hash1",
            }
        }

    def test_wraps_failures_in_state_exception(self, mocker):
        spark = mocker.Mock()
        spark.table.side_effect = RuntimeError("boom")

        with pytest.raises(StateException):
            get_processed_files(spark, "circuits")


class TestMarkFilesProcessed:
    def test_no_op_when_no_file_records(self, mocker):
        spark = mocker.Mock()
        mark_files_processed(spark, "circuits", [], "run-1")
        spark.createDataFrame.assert_not_called()

    def test_merges_file_records_into_state_table(self, mocker):
        spark = mocker.Mock()
        file_records = [
            {
                "source_file": "a.csv",
                "file_size": 100,
                "last_modified_time": "2024-01-01",
                "content_hash": "hash1",
            }
        ]

        mark_files_processed(spark, "circuits", file_records, "run-1", processing_duration_ms=500)

        spark.createDataFrame.assert_called_once()
        spark.createDataFrame.return_value.createOrReplaceTempView.assert_called_once_with("_new_state")
        spark.sql.assert_called_once()
        assert "MERGE INTO" in spark.sql.call_args[0][0]

    def test_wraps_failures_in_state_exception(self, mocker):
        spark = mocker.Mock()
        spark.createDataFrame.side_effect = RuntimeError("boom")

        with pytest.raises(StateException):
            mark_files_processed(
                spark,
                "circuits",
                [{"source_file": "a.csv", "file_size": 1, "last_modified_time": "t", "content_hash": "h"}],
                "run-1",
            )


class TestResetState:
    def test_issues_delete_for_entity(self, mocker):
        spark = mocker.Mock()
        reset_state(spark, "circuits")
        spark.sql.assert_called_once()
        sql_text = spark.sql.call_args[0][0]
        assert "DELETE FROM" in sql_text
        assert "circuits" in sql_text

    def test_wraps_failures_in_state_exception(self, mocker):
        spark = mocker.Mock()
        spark.sql.side_effect = RuntimeError("boom")

        with pytest.raises(StateException):
            reset_state(spark, "circuits")


class TestTargetTableExists:
    def test_returns_true_when_table_exists(self, mocker):
        spark = mocker.Mock()
        spark.catalog.tableExists.return_value = True
        assert target_table_exists(spark, "bronze", "circuits") is True

    def test_returns_false_when_table_missing(self, mocker):
        spark = mocker.Mock()
        spark.catalog.tableExists.return_value = False
        assert target_table_exists(spark, "bronze", "circuits") is False

    def test_wraps_failures_in_state_exception(self, mocker):
        spark = mocker.Mock()
        spark.catalog.tableExists.side_effect = RuntimeError("boom")

        with pytest.raises(StateException):
            target_table_exists(spark, "bronze", "circuits")
