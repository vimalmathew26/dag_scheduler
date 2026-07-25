"""B4: cancelling a job must be recorded honestly and raise nothing.

Before: cancelling a running job set the job to CANCELLED while the
executor was still running, so the executor then attempted
cancelled -> timed_out from outside run_job's try block. Observed: 33
unretrieved task exceptions and four run rows claiming timed_out with a
-1 sentinel exit code.

DECISION 3 governs what a cancelled run records:
  - cancelled while running: the real return code of the killed process
  - cancelled while queued, nothing ever started: NULL, not a sentinel
  - end_time set in both cases
"""

import asyncio

import pytest

from dag_scheduler.executor import Executor
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager


@pytest.fixture
def parts(persistence, tmp_path):
    pm = ProcessManager(persistence)
    return Executor(persistence, pm, LogStore(tmp_path / "test.db")), pm


class TestCancelWhileRunning:
    async def test_run_is_recorded_as_cancelled_not_timed_out(
        self, persistence, parts
    ):
        executor, pm = parts
        definition = JobDefinition(command="sleep 30", timeout=60)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        task = asyncio.create_task(executor.run_job("j", definition))
        await asyncio.sleep(1.0)

        await persistence.update_job_state("j", JobState.CANCELLED)
        await pm.kill_by_job_name("j")
        await asyncio.wait_for(task, timeout=15)

        runs = await persistence.get_runs_for_job("j")
        assert len(runs) == 1
        assert runs[0]["state"] == JobState.CANCELLED.value

    async def test_records_the_real_return_code_of_the_killed_process(
        self, persistence, parts
    ):
        executor, pm = parts
        definition = JobDefinition(command="sleep 30", timeout=60)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        task = asyncio.create_task(executor.run_job("j", definition))
        await asyncio.sleep(1.0)
        await persistence.update_job_state("j", JobState.CANCELLED)
        await pm.kill_by_job_name("j")
        await asyncio.wait_for(task, timeout=15)

        runs = await persistence.get_runs_for_job("j")
        # SIGTERM is reported as -15, never as an invented sentinel.
        assert runs[0]["exit_code"] == -15
        assert runs[0]["end_time"] is not None

    async def test_raises_nothing_into_the_task(self, persistence, parts):
        executor, pm = parts
        definition = JobDefinition(command="sleep 30", timeout=60)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        task = asyncio.create_task(executor.run_job("j", definition))
        await asyncio.sleep(1.0)
        await persistence.update_job_state("j", JobState.CANCELLED)
        await pm.kill_by_job_name("j")
        await asyncio.wait_for(task, timeout=15)

        assert task.exception() is None, (
            f"cancel leaked an exception into the dispatch task: "
            f"{task.exception()!r}"
        )

    async def test_job_stays_cancelled(self, persistence, parts):
        executor, pm = parts
        definition = JobDefinition(command="sleep 30", timeout=60)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        task = asyncio.create_task(executor.run_job("j", definition))
        await asyncio.sleep(1.0)
        await persistence.update_job_state("j", JobState.CANCELLED)
        await pm.kill_by_job_name("j")
        await asyncio.wait_for(task, timeout=15)

        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.CANCELLED


class TestCancelWhileQueued:
    async def test_records_a_run_with_no_exit_code(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)

        await persistence.record_cancelled_run("j")
        await persistence.update_job_state("j", JobState.CANCELLED)

        runs = await persistence.get_runs_for_job("j")
        assert len(runs) == 1
        assert runs[0]["state"] == JobState.CANCELLED.value
        assert runs[0]["exit_code"] is None, (
            "no process ran, so there is no exit code to report"
        )
        assert runs[0]["end_time"] is not None
        assert runs[0]["start_time"] is None, "nothing ever started"

    async def test_cancelled_job_is_not_claimable(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.update_job_state("j", JobState.CANCELLED)

        assert await persistence.claim_next_queued_job() is None
