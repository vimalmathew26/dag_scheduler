"""Structured logging, run correlation, and log batching."""

import json
import logging

import pytest

from dag_scheduler.executor import Executor
from dag_scheduler.log_store import LogStore
from dag_scheduler.logging_setup import JsonFormatter, for_run
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager
from tests.conftest import requires_posix


class TestJsonFormatter:
    def test_emits_one_json_object(self):
        record = logging.LogRecord(
            "dag_scheduler.executor", logging.INFO, "f.py", 1, "hello", (), None
        )
        payload = json.loads(JsonFormatter().format(record))
        assert payload["message"] == "hello"
        assert payload["level"] == "INFO"
        assert payload["logger"] == "dag_scheduler.executor"

    def test_adapter_fields_become_queryable_keys(self):
        record = logging.LogRecord("x", logging.INFO, "f.py", 1, "started", (), None)
        record.job = "etl"
        record.run_id = "abc-123"
        record.attempt = 2
        payload = json.loads(JsonFormatter().format(record))
        assert payload["job"] == "etl"
        assert payload["run_id"] == "abc-123"
        assert payload["attempt"] == 2

    def test_exception_is_included(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            record = logging.LogRecord("x", logging.ERROR, "f.py", 1, "failed", (), sys.exc_info())
        payload = json.loads(JsonFormatter().format(record))
        assert "boom" in payload["exception"]


class TestRunCorrelation:
    def test_adapter_prefixes_and_attaches_fields(self, caplog):
        logger = logging.getLogger("dag_scheduler.test")
        log = for_run(logger, "etl", "abcdef12-3456", 2)
        with caplog.at_level(logging.INFO, logger="dag_scheduler.test"):
            log.info("Starting")

        record = caplog.records[0]
        assert record.job == "etl"
        assert record.run_id == "abcdef12-3456"
        assert record.attempt == 2
        assert "abcdef12" in record.message

    async def test_every_line_of_a_run_carries_its_run_id(self, persistence, tmp_path, caplog):
        """The point of correlation: one filter returns the whole run."""
        executor = Executor(persistence, ProcessManager(persistence), LogStore(tmp_path / "t.db"))
        definition = JobDefinition(command="echo hi", timeout=10)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        with caplog.at_level(logging.INFO, logger="dag_scheduler.executor"):
            await executor.run_job("j", definition, attempt=1)

        runs = await persistence.get_runs_for_job("j")
        run_id = runs[0]["run_id"]
        correlated = [r for r in caplog.records if getattr(r, "run_id", None) == run_id]
        assert len(correlated) >= 2, "start and finish must both be attributable to the run"
        assert all(r.job == "j" for r in correlated)


class TestLogBatching:
    @requires_posix
    async def test_batched_writes_preserve_order(self, persistence, tmp_path):
        executor = Executor(persistence, ProcessManager(persistence), LogStore(persistence.db_path))
        store = LogStore(persistence.db_path)
        definition = JobDefinition(
            command="for i in $(seq 1 250); do echo line$i; done", timeout=30
        )
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        await executor.run_job("j", definition)

        runs = await persistence.get_runs_for_job("j")
        entries = await store.get_logs(runs[0]["run_id"])
        assert len(entries) == 250
        assert entries[0][1].strip() == "line1"
        assert entries[-1][1].strip() == "line250"

    async def test_batch_rejects_a_bad_stream_name(self, persistence):
        store = LogStore(persistence.db_path)
        with pytest.raises(ValueError):
            await store.store_log_chunks("r", [("stdout", "a"), ("nope", "b")])

    async def test_empty_batch_is_a_no_op(self, persistence):
        await LogStore(persistence.db_path).store_log_chunks("r", [])


class TestIndexes:
    async def test_expected_indexes_exist(self, persistence):
        import aiosqlite

        async with (
            aiosqlite.connect(persistence.db_path) as db,
            db.execute("SELECT name FROM sqlite_master WHERE type='index'") as cursor,
        ):
            names = {row[0] for row in await cursor.fetchall()}

        assert {
            "idx_job_runs_job_name",
            "idx_job_runs_state",
            "idx_job_logs_run_id",
            "idx_jobs_state",
        } <= names

    async def test_run_history_lookup_uses_the_index(self, persistence):
        import aiosqlite

        async with (
            aiosqlite.connect(persistence.db_path) as db,
            db.execute(
                "EXPLAIN QUERY PLAN SELECT run_id FROM job_runs "
                "WHERE job_name = 'x' ORDER BY start_time DESC"
            ) as cursor,
        ):
            plan = " ".join(str(row) for row in await cursor.fetchall())
        assert "idx_job_runs_job_name" in plan, f"full scan: {plan}"
