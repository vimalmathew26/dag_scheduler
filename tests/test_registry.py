"""Registry reload and the added/removed/changed diff."""

import asyncio

import pytest

from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.registry import Registry


def write(d, name, body):
    (d / name).write_text(body)


@pytest.fixture
def registry(persistence, tmp_path):
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir()
    return Registry(persistence, jobs_dir), jobs_dir


class TestInitialLoad:
    async def test_loads_and_persists(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")

        await reg.load_initial()

        assert set(reg.jobs) == {"x"}
        assert set(await persistence.get_all_db_jobs()) == {"x"}

    async def test_empty_directory_is_not_an_error(self, registry):
        reg, _ = registry
        await reg.load_initial()
        assert reg.jobs == {}


class TestReloadDiff:
    async def test_added_job_is_persisted(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()

        write(d, "b.yaml", "jobs:\n  y:\n    command: 'echo'\n")
        await reg.reload()

        assert set(await persistence.get_all_db_jobs()) == {"x", "y"}

    async def test_changed_command_is_detected(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'old'\n")
        await reg.load_initial()

        write(d, "a.yaml", "jobs:\n  x:\n    command: 'new'\n")
        await reg.reload()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["x"]["definition"]["command"] == "new"

    async def test_changed_tags_are_detected(self, registry, persistence):
        """The diff compares whole models, not just commands."""
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n    tags: ['a']\n")
        await reg.load_initial()

        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n    tags: ['b']\n")
        await reg.reload()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["x"]["definition"]["tags"] == ["b"]

    async def test_unchanged_job_survives_a_reload(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()
        await persistence.update_job_state("x", JobState.QUEUED)

        await reg.reload()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["x"]["state"] is JobState.QUEUED

    async def test_removed_defined_job_is_deleted(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()

        (d / "a.yaml").unlink()
        await reg.reload()

        assert "x" not in await persistence.get_all_db_jobs()

    async def test_removed_queued_job_becomes_blocked(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()
        await persistence.update_job_state("x", JobState.QUEUED)

        (d / "a.yaml").unlink()
        await reg.reload()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["x"]["state"] is JobState.BLOCKED_UNRESOLVABLE

    async def test_removed_running_job_is_left_alone(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()
        await persistence.update_job_state("x", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        (d / "a.yaml").unlink()
        await reg.reload()

        jobs = await persistence.get_all_db_jobs()
        assert jobs["x"]["state"] is JobState.RUNNING, (
            "a running job must be allowed to finish"
        )

    async def test_known_job_names_tracks_the_snapshot(self, registry):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()
        assert reg.known_job_names() == {"x"}

        (d / "a.yaml").unlink()
        await reg.reload()
        assert reg.known_job_names() == set()

    async def test_reload_is_idempotent(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()

        await reg.reload()
        first = await persistence.get_all_db_jobs()
        await reg.reload()
        second = await persistence.get_all_db_jobs()

        assert first == second


class TestReloadConcurrency:
    async def test_snapshot_is_never_half_applied(self, registry, persistence):
        """A reader must not see the new namespace before the database
        rows behind it have been reconciled."""
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()

        observations = []

        async def observe():
            for _ in range(60):
                snap = await reg.snapshot()
                db = await persistence.get_all_db_jobs()
                observations.append((set(snap), set(db)))
                await asyncio.sleep(0.005)

        watcher = asyncio.create_task(observe())
        for i in range(6):
            write(d, "b.yaml", f"jobs:\n  y{i}:\n    command: 'echo'\n")
            await reg.reload()
            await asyncio.sleep(0.01)
        await watcher

        for snap, db in observations:
            assert snap <= db, (
                f"registry advertised {snap - db} before the database had it"
            )

    async def test_concurrent_reloads_serialise(self, registry, persistence):
        reg, d = registry
        write(d, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        await reg.load_initial()

        await asyncio.gather(*(reg.reload() for _ in range(8)))

        assert set(reg.jobs) == {"x"}
        assert set(await persistence.get_all_db_jobs()) == {"x"}

    async def test_parsing_does_not_block_the_event_loop(self, registry):
        """Parsing is blocking file I/O and must run off the loop."""
        reg, d = registry
        for i in range(30):
            write(d, f"f{i}.yaml", f"jobs:\n  j{i}:\n    command: 'echo'\n")

        ticks = 0

        async def heartbeat():
            nonlocal ticks
            for _ in range(20):
                await asyncio.sleep(0.001)
                ticks += 1

        await asyncio.gather(reg.reload(), heartbeat())
        assert ticks == 20
