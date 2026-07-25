"""The /metrics endpoint and the counters behind it."""

import pytest
from httpx import ASGITransport, AsyncClient

from dag_scheduler import metrics
from dag_scheduler.api import app, init_api
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager
from dag_scheduler.scheduler import Scheduler


@pytest.fixture(autouse=True)
def clean_metrics():
    metrics.reset()
    yield
    metrics.reset()


class Reg:
    jobs = {}

    def get_job(self, name):
        return self.jobs.get(name)

    def get_all_jobs(self):
        return dict(self.jobs)

    def known_job_names(self):
        return set(self.jobs)

    async def snapshot(self):
        return dict(self.jobs)


@pytest.fixture
async def client(persistence):
    init_api(
        Scheduler(persistence, Reg(), None),
        Reg(),
        persistence,
        LogStore(persistence.db_path),
        ProcessManager(persistence),
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


class TestRendering:
    def test_declared_series_appear_even_at_zero(self):
        body = metrics.render()
        for name in [
            "dag_dispatches_total",
            "dag_runs_total",
            "dag_retries_total",
            "dag_reload_failures_total",
            "dag_scheduler_loop_errors_total",
        ]:
            assert f"# TYPE {name}" in body
            assert f"{name} 0.0" in body or f"{name}{{" in body

    def test_counters_increment(self):
        metrics.increment("dag_dispatches_total")
        metrics.increment("dag_dispatches_total", 2)
        assert "dag_dispatches_total 3.0" in metrics.render()

    def test_labels_produce_separate_series(self):
        metrics.increment("dag_runs_total", state="done")
        metrics.increment("dag_runs_total", state="done")
        metrics.increment("dag_runs_total", state="failed")
        body = metrics.render()
        assert 'dag_runs_total{state="done"} 2.0' in body
        assert 'dag_runs_total{state="failed"} 1.0' in body

    def test_help_and_type_precede_every_series(self):
        metrics.increment("dag_retries_total")
        lines = metrics.render().splitlines()
        i = next(i for i, line in enumerate(lines) if line.startswith("dag_retries_total "))
        assert lines[i - 1].startswith("# TYPE dag_retries_total")
        assert lines[i - 2].startswith("# HELP dag_retries_total")


class TestEndpoint:
    async def test_served_as_plain_text(self, client):
        r = await client.get("/metrics")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain")

    async def test_gauges_reflect_the_database(self, client, persistence):
        for name, state in [
            ("q1", JobState.QUEUED),
            ("q2", JobState.QUEUED),
            ("r1", JobState.RUNNING),
        ]:
            await persistence.upsert_job(name, JobDefinition(command="true"))
            await persistence.update_job_state(name, JobState.QUEUED)
            if state is JobState.RUNNING:
                await persistence.claim_next_queued_job()

        body = (await client.get("/metrics")).text

        assert "dag_queued_jobs 2" in body
        assert "dag_running_jobs 1" in body

    async def test_empty_database_reports_zero(self, client):
        body = (await client.get("/metrics")).text
        assert "dag_queued_jobs 0" in body
        assert "dag_running_jobs 0" in body

    async def test_metrics_is_a_read_and_needs_no_token(self, client):
        assert (await client.get("/metrics")).status_code == 200


class TestInstrumentation:
    async def test_a_run_increments_the_outcome_counter(self, persistence, tmp_path):
        from dag_scheduler.executor import Executor

        executor = Executor(persistence, ProcessManager(persistence), LogStore(tmp_path / "t.db"))
        definition = JobDefinition(command="exit 0", timeout=10)
        await persistence.upsert_job("j", definition)
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        await executor.run_job("j", definition)

        assert 'dag_runs_total{state="done"} 1.0' in metrics.render()

    async def test_a_retry_increments_the_retry_counter(self):
        from dag_scheduler.models import JobRun, RetryPolicy
        from dag_scheduler.retry_engine import RetryEngine

        class Sched:
            async def enqueue_job(self, *a, **kw):
                pass

        engine = RetryEngine(Sched())
        definition = JobDefinition(
            command="exit 1", retry=RetryPolicy(max_attempts=3, backoff_base=1.0)
        )
        run = JobRun(job_name="j", run_id="r", state=JobState.FAILED, attempt=1)

        await engine.handle_retry(definition, run, 1)

        assert "dag_retries_total 1.0" in metrics.render()
