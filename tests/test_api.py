"""API tests over ASGI, with no live server and no real database.

These were impossible before the config work: six routes opened
aiosqlite connections against the module-global config.DB_PATH.
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dag_scheduler.api import app, init_api
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobRun, JobState
from dag_scheduler.process_manager import ProcessManager
from dag_scheduler.scheduler import Scheduler


class FakeRegistry:
    def __init__(self, jobs=None):
        self.jobs = jobs or {}

    def get_job(self, name):
        return self.jobs.get(name)

    def get_all_jobs(self):
        return dict(self.jobs)

    def known_job_names(self):
        return set(self.jobs)


@pytest_asyncio.fixture
async def client(persistence, tmp_path):
    registry = FakeRegistry()
    log_store = LogStore(persistence.db_path)
    pm = ProcessManager(persistence)
    scheduler = Scheduler(persistence, registry, executor=None)
    init_api(scheduler, registry, persistence, log_store, pm)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        c.registry = registry
        yield c


async def seed(persistence, name, **kw):
    definition = JobDefinition(command=kw.pop("command", "echo hi"), **kw)
    await persistence.upsert_job(name, definition)
    return definition


class TestReadEndpoints:
    async def test_health(self, client):
        r = await client.get("/health")
        assert r.status_code == 200 and r.json() == {"status": "ok"}

    async def test_root(self, client):
        assert (await client.get("/")).status_code == 200

    async def test_list_jobs_empty(self, client):
        r = await client.get("/jobs")
        assert r.status_code == 200 and r.json() == []

    async def test_list_jobs(self, client, persistence):
        await seed(persistence, "a", tags=["x"], priority=3)
        r = await client.get("/jobs")
        assert [j["name"] for j in r.json()] == ["a"]
        assert r.json()[0]["priority"] == 3

    async def test_filter_by_state(self, client, persistence):
        await seed(persistence, "a")
        await seed(persistence, "b")
        await persistence.update_job_state("b", JobState.QUEUED)

        r = await client.get("/jobs", params={"state": "queued"})
        assert [j["name"] for j in r.json()] == ["b"]

    async def test_filter_by_tag(self, client, persistence):
        await seed(persistence, "a", tags=["etl"])
        await seed(persistence, "b", tags=["ops"])

        r = await client.get("/jobs", params={"tag": "etl"})
        assert [j["name"] for j in r.json()] == ["a"]

    async def test_job_detail(self, client, persistence):
        await seed(persistence, "a")
        r = await client.get("/jobs/a")
        assert r.status_code == 200
        assert r.json()["name"] == "a"
        assert r.json()["last_run"] is None

    async def test_job_detail_404(self, client):
        assert (await client.get("/jobs/nope")).status_code == 404

    async def test_runs_404_for_unknown_job(self, client):
        assert (await client.get("/jobs/nope/runs")).status_code == 404

    async def test_runs_listed_newest_first(self, client, persistence):
        await seed(persistence, "a")
        for i, ts in enumerate(["2026-01-01 00:00:00", "2026-01-02 00:00:00"]):
            await persistence.record_run(
                JobRun(job_name="a", run_id=f"r{i}", state=JobState.RUNNING,
                       start_time=ts, attempt=i + 1)
            )
        r = await client.get("/jobs/a/runs")
        assert [run["run_id"] for run in r.json()] == ["r1", "r0"]

    async def test_logs_404_for_unknown_run(self, client, persistence):
        await seed(persistence, "a")
        assert (await client.get("/jobs/a/runs/nope/logs")).status_code == 404


class TestStats:
    async def test_stats_on_an_empty_database(self, client):
        r = await client.get("/stats")
        assert r.status_code == 200
        assert r.json()["total_runs"] == 0

    async def test_pass_rate(self, client, persistence):
        await seed(persistence, "a")
        for i, state in enumerate([JobState.DONE, JobState.DONE, JobState.FAILED]):
            await persistence.record_run(
                JobRun(job_name="a", run_id=f"r{i}", state=JobState.RUNNING,
                       start_time="2026-01-01 00:00:00")
            )
            await persistence.finalize_run(f"r{i}", state, 0)

        body = (await client.get("/stats")).json()
        assert body["total_runs"] == 3
        assert body["pass_rate"] == pytest.approx(2 / 3)


class TestTrigger:
    async def test_404_for_unknown_job(self, client):
        assert (await client.post("/jobs/nope/trigger")).status_code == 404

    async def test_queues_the_job(self, client, persistence):
        await seed(persistence, "a")
        client.registry.jobs["a"] = JobDefinition(command="echo hi")

        r = await client.post("/jobs/a/trigger")

        assert r.status_code == 200
        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.QUEUED


class TestCancel:
    async def test_404_for_unknown_job(self, client):
        assert (await client.post("/jobs/nope/cancel")).status_code == 404

    @pytest.mark.parametrize("state", [JobState.DEFINED, JobState.DONE])
    async def test_400_when_not_cancellable(self, client, persistence, state):
        await seed(persistence, "a")
        if state is JobState.DONE:
            await persistence.update_job_state("a", JobState.QUEUED)
            await persistence.claim_next_queued_job()
            await persistence.update_job_state("a", JobState.DONE)

        r = await client.post("/jobs/a/cancel")
        assert r.status_code == 400

    async def test_cancelling_a_queued_job_records_a_null_exit_code(
        self, client, persistence
    ):
        await seed(persistence, "a")
        await persistence.update_job_state("a", JobState.QUEUED)

        r = await client.post("/jobs/a/cancel")

        assert r.status_code == 200
        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.CANCELLED
        runs = await persistence.get_runs_for_job("a")
        assert len(runs) == 1
        assert runs[0]["state"] == "cancelled"
        assert runs[0]["exit_code"] is None
        assert runs[0]["start_time"] is None


class TestReset:
    async def test_resets_a_terminal_job(self, client, persistence):
        await seed(persistence, "a")
        await persistence.update_job_state("a", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("a", JobState.DONE)

        r = await client.post("/jobs/a/reset")

        assert r.status_code == 200
        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.DEFINED

    async def test_400_for_a_non_terminal_job(self, client, persistence):
        await seed(persistence, "a")
        await persistence.update_job_state("a", JobState.QUEUED)
        assert (await client.post("/jobs/a/reset")).status_code == 400

    async def test_404_for_unknown_job(self, client):
        assert (await client.post("/jobs/nope/reset")).status_code == 404
