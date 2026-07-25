"""B5: crash recovery must reconcile runs, not just jobs.

handle_crash_recovery only ever read and wrote the jobs table. Observed:
after a SIGKILL mid-run and a restart, the job was correctly marked
unknown while four job_runs rows sat permanently in state 'running' with
no end_time, understating the pass rate in /stats forever.
"""

import pytest

from dag_scheduler.models import JobDefinition, JobRun, JobState
from dag_scheduler.process_manager import ProcessManager


@pytest.fixture
def manager(persistence):
    return ProcessManager(persistence)


async def seed_run(persistence, job_name, state, run_id):
    await persistence.record_run(
        JobRun(
            job_name=job_name,
            run_id=run_id,
            state=state,
            start_time="2026-01-01 00:00:00",
            attempt=1,
        )
    )


class TestCrashRecovery:
    async def test_running_job_becomes_unknown(self, persistence, manager):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        await manager.handle_crash_recovery()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.UNKNOWN

    @pytest.mark.parametrize(
        "state", [JobState.QUEUED, JobState.WAITING, JobState.DONE, JobState.FAILED]
    )
    async def test_other_states_are_untouched(self, persistence, manager, state):
        """Only RUNNING is orphaned by a restart."""
        await persistence.upsert_job("j", JobDefinition(command="true"))

        # Walk a legal path to the target state.
        if state is JobState.WAITING:
            await persistence.update_job_state("j", JobState.WAITING)
        else:
            await persistence.update_job_state("j", JobState.QUEUED)
            if state is not JobState.QUEUED:
                await persistence.claim_next_queued_job()
                await persistence.update_job_state("j", state)

        await manager.handle_crash_recovery()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is state

    async def test_dangling_run_rows_are_finalized(self, persistence, manager):
        """The regression test for B5."""
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await seed_run(persistence, "j", JobState.RUNNING, "run-1")
        await seed_run(persistence, "j", JobState.RUNNING, "run-2")

        await manager.handle_crash_recovery()

        runs = await persistence.get_runs_for_job("j")
        assert len(runs) == 2
        for run in runs:
            assert run["state"] == JobState.UNKNOWN.value, (
                "an orphaned run must not stay in 'running' forever"
            )
            assert run["end_time"] is not None
            assert run["exit_code"] is None, (
                "the process was never reaped, so there is no exit code"
            )

    async def test_completed_runs_are_untouched(self, persistence, manager):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await seed_run(persistence, "j", JobState.RUNNING, "done-run")
        await persistence.finalize_run("done-run", JobState.DONE, 0)

        await manager.handle_crash_recovery()

        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["state"] == JobState.DONE.value
        assert runs[0]["exit_code"] == 0

    async def test_is_idempotent(self, persistence, manager):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await seed_run(persistence, "j", JobState.RUNNING, "run-1")

        await manager.handle_crash_recovery()
        first = await persistence.get_runs_for_job("j")
        await manager.handle_crash_recovery()
        second = await persistence.get_runs_for_job("j")

        assert first == second

    async def test_no_runs_left_in_running_after_recovery(self, persistence, manager):
        for i in range(4):
            await seed_run(persistence, "j", JobState.RUNNING, f"run-{i}")

        await manager.handle_crash_recovery()

        dangling = await persistence.count_runs_in_state(JobState.RUNNING)
        assert dangling == 0
