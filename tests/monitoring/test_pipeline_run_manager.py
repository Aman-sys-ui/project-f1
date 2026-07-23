from datetime import datetime

from src.monitoring.pipeline_run_manager import complete_run, fail_run, start_run


class TestStartRun:
    def test_writes_a_running_record_and_returns_run_id(self, mocker):
        spark = mocker.Mock()

        run_id, start_time = start_run(spark, pipeline_name="bronze_ingestion", entity_name="circuits")

        assert isinstance(run_id, str)
        assert isinstance(start_time, datetime)
        spark.createDataFrame.return_value.write.mode.return_value.saveAsTable.assert_called_once()


class TestCompleteRun:
    def test_persists_the_status_argument_not_a_hardcoded_success(self, mocker):
        """Regression test: complete_run() previously hardcoded status='SUCCESS'
        in its UPDATE statement, ignoring the status it was called with — so a
        SKIPPED run was recorded as SUCCESS.
        """
        spark = mocker.Mock()

        complete_run(
            spark=spark,
            run_id="run-1",
            start_time=datetime(2026, 1, 1),
            status="SKIPPED",
            records_processed=0,
            files_processed=0,
        )

        sql_text = spark.sql.call_args[0][0]
        assert "status = 'SKIPPED'" in sql_text
        assert "status = 'SUCCESS'" not in sql_text

    def test_persists_success_status(self, mocker):
        spark = mocker.Mock()

        complete_run(
            spark=spark,
            run_id="run-1",
            start_time=datetime(2026, 1, 1),
            status="SUCCESS",
            records_processed=10,
            files_processed=2,
        )

        sql_text = spark.sql.call_args[0][0]
        assert "status = 'SUCCESS'" in sql_text
        assert "records_processed = 10" in sql_text
        assert "files_processed = 2" in sql_text

    def test_targets_the_correct_run_id(self, mocker):
        spark = mocker.Mock()

        complete_run(
            spark=spark,
            run_id="run-42",
            start_time=datetime(2026, 1, 1),
            status="SUCCESS",
            records_processed=0,
            files_processed=0,
        )

        sql_text = spark.sql.call_args[0][0]
        assert "WHERE run_id = 'run-42'" in sql_text


class TestFailRun:
    def test_records_failed_status_and_error_message(self, mocker):
        spark = mocker.Mock()

        fail_run(
            spark=spark,
            run_id="run-1",
            start_time=datetime(2026, 1, 1),
            error_message="boom",
        )

        sql_text = spark.sql.call_args[0][0]
        assert "status = 'FAILED'" in sql_text
        assert "boom" in sql_text

    def test_escapes_single_quotes_in_error_message(self, mocker):
        spark = mocker.Mock()

        fail_run(
            spark=spark,
            run_id="run-1",
            start_time=datetime(2026, 1, 1),
            error_message="entity 'circuits' failed",
        )

        sql_text = spark.sql.call_args[0][0]
        assert "entity ''circuits'' failed" in sql_text
