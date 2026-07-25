"""Daemon wiring and the shutdown sequence.

__main__.py had no coverage at all, which is awkward for the module that
owns the shutdown ordering: the defect there was abandoned subprocesses and
run rows left mid-write.
"""

import asyncio
import signal

import pytest

from dag_scheduler.__main__ import Daemon, _DaemonManagedServer
from dag_scheduler.models import JobDefinition, JobState


@pytest.fixture
def daemon(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    return Daemon(db_path=tmp_path / "d.db", jobs_dir=jobs)


class TestWiring:
    def test_components_are_connected(self, daemon):
        assert daemon.executor.retry_engine is daemon.retry_engine
        assert daemon.executor.scheduler is daemon.scheduler
        assert daemon.scheduler.executor is daemon.executor
        assert daemon.retry_engine.scheduler is daemon.scheduler

    def test_every_component_shares_one_database(self, daemon, tmp_path):
        assert daemon.persistence.db_path == tmp_path / "d.db"
        assert daemon.log_store.db_path == tmp_path / "d.db"

    def test_directories_are_created_eagerly(self, tmp_path):
        target = tmp_path / "fresh" / "sub"
        Daemon(db_path=target / "d.db", jobs_dir=target / "jobs")
        assert (target / "jobs").is_dir()

    def test_uvicorn_signal_handlers_are_disabled(self):
        """The daemon owns the shutdown sequence, not uvicorn."""
        import uvicorn

        config = uvicorn.Config(app=None, host="127.0.0.1", port=1)
        server = _DaemonManagedServer(config)
        assert server.install_signal_handlers() is None
        assert issubclass(_DaemonManagedServer, uvicorn.Server)


class TestShutdown:
    async def test_shutdown_is_idempotent(self, daemon):
        await daemon.persistence.setup()
        await daemon.shutdown()
        await daemon.shutdown(signal.SIGTERM)
        assert daemon.stop_event.is_set()

    async def test_shutdown_stops_the_scheduler(self, daemon):
        await daemon.persistence.setup()
        await daemon.scheduler.start()
        assert daemon.scheduler.running is True

        await daemon.shutdown(signal.SIGTERM)

        assert daemon.scheduler.running is False
        assert daemon.scheduler._loop_task.done()
        assert daemon.scheduler._aging_task.done()

    async def test_shutdown_terminates_running_job_processes(self, daemon):
        """The defect: shutdown cancelled the supervising tasks and left
        the processes running."""
        await daemon.persistence.setup()
        process = await asyncio.create_subprocess_shell("sleep 30", start_new_session=True)
        await daemon.process_manager.register_process("j", "run-1", process)

        await daemon.shutdown(signal.SIGTERM)

        await asyncio.wait_for(process.wait(), timeout=10)
        assert process.returncode is not None

    async def test_shutdown_sets_the_stop_event_last(self, daemon):
        await daemon.persistence.setup()
        assert not daemon.stop_event.is_set()
        await daemon.shutdown()
        assert daemon.stop_event.is_set()

    async def test_shutdown_waits_for_in_flight_jobs_to_finalize(self, daemon):
        """A run must not be abandoned between record_run and finalize_run."""
        await daemon.persistence.setup()
        definition = JobDefinition(command="sleep 0.5", timeout=30)
        await daemon.persistence.upsert_job("j", definition)
        await daemon.persistence.update_job_state("j", JobState.QUEUED)
        await daemon.persistence.claim_next_queued_job()

        task = daemon.scheduler._spawn(daemon.executor.run_job("j", definition))
        await asyncio.sleep(0.2)

        await daemon.shutdown(signal.SIGTERM)
        await asyncio.sleep(0.1)

        assert task.done()
        dangling = await daemon.persistence.count_runs_in_state(JobState.RUNNING)
        assert dangling == 0, "a run row was left mid-write by shutdown"


class TestStartup:
    async def test_crash_recovery_runs_before_definitions_load(self, daemon):
        """Order matters: a job left running by a previous lifetime must be
        reconciled before anything can be dispatched over the top of it."""
        await daemon.persistence.setup()
        await daemon.persistence.upsert_job("j", JobDefinition(command="true"))
        await daemon.persistence.update_job_state("j", JobState.QUEUED)
        await daemon.persistence.claim_next_queued_job()

        await daemon.process_manager.handle_crash_recovery()

        jobs = await daemon.persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.UNKNOWN

    async def test_a_bad_definition_file_does_not_stop_startup(self, daemon, tmp_path):
        (tmp_path / "jobs" / "broken.yaml").write_text("jobs:\n  x:\n   - [oops\n")
        (tmp_path / "jobs" / "good.yaml").write_text("jobs:\n  good:\n    command: 'true'\n")
        await daemon.persistence.setup()

        await daemon.registry.load_initial()

        assert "good" in daemon.registry.jobs
