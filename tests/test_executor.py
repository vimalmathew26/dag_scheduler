"""Executor: exit codes, log capture, and concurrency."""

import asyncio
import time

import pytest

from dag_scheduler.executor import Executor
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager


@pytest.fixture
def executor(persistence, tmp_path):
    return Executor(persistence, ProcessManager(persistence), LogStore(tmp_path / "test.db"))


@pytest.fixture
def log_store(tmp_path):
    return LogStore(tmp_path / "test.db")


async def run(executor, persistence, name, command, **kw):
    definition = JobDefinition(command=command, timeout=kw.pop("timeout", 30), **kw)
    await persistence.upsert_job(name, definition)
    await persistence.update_job_state(name, JobState.QUEUED)
    await persistence.claim_next_queued_job()
    await executor.run_job(name, definition, attempt=kw.pop("attempt", 1))


class TestOutcomes:
    async def test_success(self, executor, persistence):
        await run(executor, persistence, "j", "exit 0")
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.DONE
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["exit_code"] == 0

    async def test_failure_records_the_real_exit_code(self, executor, persistence):
        await run(executor, persistence, "j", "exit 3")
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.FAILED
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["exit_code"] == 3

    async def test_nonexistent_command_exits_127(self, executor, persistence):
        await run(executor, persistence, "j", "definitely_not_a_command_xyz")
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["exit_code"] == 127
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.FAILED

    async def test_run_records_the_attempt_number(self, executor, persistence):
        await run(executor, persistence, "j", "exit 0", attempt=4)
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["attempt"] == 4

    async def test_every_run_gets_start_and_end_times(self, executor, persistence):
        await run(executor, persistence, "j", "exit 0")
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["start_time"] and runs[0]["end_time"]


class TestLogCapture:
    async def test_stdout_is_stored(self, executor, persistence, log_store):
        await run(executor, persistence, "j", "echo hello_stdout")
        runs = await persistence.get_runs_for_job("j")
        entries = await log_store.get_logs(runs[0]["run_id"])
        assert any("hello_stdout" in chunk for _, chunk, _ in entries)

    async def test_stderr_is_stored_and_tagged(self, executor, persistence, log_store):
        await run(executor, persistence, "j", "echo oops >&2")
        runs = await persistence.get_runs_for_job("j")
        entries = await log_store.get_logs(runs[0]["run_id"])
        streams = {stream for stream, _, _ in entries}
        assert "stderr" in streams
        assert any("oops" in chunk for stream, chunk, _ in entries if stream == "stderr")

    async def test_both_streams_are_captured(self, executor, persistence, log_store):
        await run(executor, persistence, "j", "echo out; echo err >&2")
        runs = await persistence.get_runs_for_job("j")
        entries = await log_store.get_logs(runs[0]["run_id"])
        assert {stream for stream, _, _ in entries} == {"stdout", "stderr"}

    async def test_a_chatty_job_completes(self, executor, persistence, log_store):
        await run(executor, persistence, "j", "for i in $(seq 1 500); do echo line$i; done")
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.DONE
        runs = await persistence.get_runs_for_job("j")
        entries = await log_store.get_logs(runs[0]["run_id"])
        assert len(entries) == 500

    async def test_no_output_means_no_log_rows(self, executor, persistence, log_store):
        await run(executor, persistence, "j", "exit 0")
        runs = await persistence.get_runs_for_job("j")
        assert await log_store.get_logs(runs[0]["run_id"]) == []

    async def test_invalid_stream_name_is_rejected(self, log_store):
        with pytest.raises(ValueError):
            await log_store.store_log_chunk("r", "stdlol", "x")


class TestConcurrency:
    async def test_at_capacity_reflects_the_semaphore(self, executor):
        assert executor.at_capacity() is False
        holders = [asyncio.create_task(executor.semaphore.acquire()) for _ in range(4)]
        await asyncio.sleep(0.05)
        assert executor.at_capacity() is True
        for _ in range(4):
            executor.semaphore.release()
        for h in holders:
            await h

    async def test_semaphore_limits_parallelism(self, persistence, tmp_path, monkeypatch):
        """With two slots, four one-second jobs take about two seconds."""
        executor = Executor(
            persistence, ProcessManager(persistence), LogStore(tmp_path / "test.db")
        )
        executor.semaphore = asyncio.Semaphore(2)

        for i in range(4):
            d = JobDefinition(command="sleep 1", timeout=30)
            await persistence.upsert_job(f"j{i}", d)
            await persistence.update_job_state(f"j{i}", JobState.QUEUED)
            await persistence.claim_next_queued_job()

        d = JobDefinition(command="sleep 1", timeout=30)
        started = time.monotonic()
        await asyncio.gather(*(executor.run_job(f"j{i}", d) for i in range(4)))
        elapsed = time.monotonic() - started

        assert 1.8 < elapsed < 4.0, f"expected serialisation into 2 waves, took {elapsed:.2f}s"
