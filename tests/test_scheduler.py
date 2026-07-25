"""Scheduler: enqueue, dependency fan-out, and the dispatch loop."""

import asyncio

import pytest

from dag_scheduler.executor import Executor
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager
from dag_scheduler.scheduler import Scheduler


class FakeRegistry:
    def __init__(self, jobs):
        self.jobs = dict(jobs)

    def get_job(self, name):
        return self.jobs.get(name)

    def get_all_jobs(self):
        return dict(self.jobs)

    def known_job_names(self):
        return set(self.jobs)

    async def snapshot(self):
        return dict(self.jobs)


class RecordingExecutor:
    """Stands in for the executor, recording dispatches."""

    def __init__(self, capacity=False):
        self.dispatched = []
        self._capacity = capacity

    def at_capacity(self):
        return self._capacity

    async def run_job(self, name, definition, attempt=1):
        self.dispatched.append((name, attempt))


def job(*deps, **kw):
    return JobDefinition(command=kw.pop("command", "true"), depends_on=list(deps), **kw)


async def seed(persistence, **jobs):
    for name, definition in jobs.items():
        await persistence.upsert_job(name, definition)


class TestEnqueue:
    async def test_bypass_deps_queues_immediately(self, persistence):
        await seed(persistence, a=job("missing_parent"))
        sched = Scheduler(persistence, FakeRegistry({"a": job("missing_parent")}), None)

        await sched.enqueue_job("a", bypass_deps=True)

        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.QUEUED

    async def test_unmet_dependencies_go_to_waiting(self, persistence):
        await seed(persistence, parent=job(), child=job("parent"))
        reg = FakeRegistry({"parent": job(), "child": job("parent")})
        sched = Scheduler(persistence, reg, None)

        await sched.enqueue_job("child")

        jobs = await persistence.get_all_db_jobs()
        assert jobs["child"]["state"] is JobState.WAITING

    async def test_met_dependencies_go_to_queued(self, persistence):
        await seed(persistence, parent=job(), child=job("parent"))
        await persistence.update_job_state("parent", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("parent", JobState.DONE)
        reg = FakeRegistry({"parent": job(), "child": job("parent")})
        sched = Scheduler(persistence, reg, None)

        await sched.enqueue_job("child")

        jobs = await persistence.get_all_db_jobs()
        assert jobs["child"]["state"] is JobState.QUEUED

    async def test_enqueueing_a_job_with_no_definition_raises(self, persistence):
        """Documents current behaviour, which is wrong. See FINDINGS.md F4.

        enqueue_job sends a job with no definition straight to
        BLOCKED_UNRESOLVABLE, but a DEFINED job cannot legally go there, so
        it raises instead. Unreachable from the API, which 404s first, but
        reachable from a retry whose definition was removed mid-backoff.
        """
        from dag_scheduler.persistence import InvalidTransitionError

        await seed(persistence, a=job())
        sched = Scheduler(persistence, FakeRegistry({}), None)

        with pytest.raises(InvalidTransitionError):
            await sched.enqueue_job("a")

    async def test_queued_job_with_no_definition_becomes_blocked(self, persistence):
        """The same path works from QUEUED, which is a legal source."""
        await seed(persistence, a=job())
        await persistence.update_job_state("a", JobState.QUEUED)
        sched = Scheduler(persistence, FakeRegistry({}), None)

        await sched.enqueue_job("a")

        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.BLOCKED_UNRESOLVABLE

    @pytest.mark.parametrize(
        "terminal", [JobState.DONE, JobState.FAILED, JobState.TIMED_OUT,
                     JobState.UNKNOWN, JobState.CANCELLED])
    async def test_terminal_job_is_reset_before_re_enqueue(self, persistence, terminal):
        await seed(persistence, a=job())
        await persistence.update_job_state("a", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("a", terminal)
        sched = Scheduler(persistence, FakeRegistry({"a": job()}), None)

        await sched.enqueue_job("a", bypass_deps=True)

        jobs = await persistence.get_all_db_jobs()
        assert jobs["a"]["state"] is JobState.QUEUED


class TestFanOut:
    async def test_completion_queues_a_ready_dependent(self, persistence):
        jobs = {"parent": job(), "child": job("parent")}
        await seed(persistence, **jobs)
        await persistence.update_job_state("parent", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("parent", JobState.DONE)
        await persistence.update_job_state("child", JobState.WAITING)
        sched = Scheduler(persistence, FakeRegistry(jobs), None)

        await sched.handle_job_completion("parent")

        db = await persistence.get_all_db_jobs()
        assert db["child"]["state"] is JobState.QUEUED

    async def test_dependent_with_an_outstanding_parent_stays_waiting(self, persistence):
        jobs = {"p1": job(), "p2": job(), "child": job("p1", "p2")}
        await seed(persistence, **jobs)
        await persistence.update_job_state("p1", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("p1", JobState.DONE)
        await persistence.update_job_state("child", JobState.WAITING)
        sched = Scheduler(persistence, FakeRegistry(jobs), None)

        await sched.handle_job_completion("p1")

        db = await persistence.get_all_db_jobs()
        assert db["child"]["state"] is JobState.WAITING

    async def test_unknown_completed_job_is_a_no_op(self, persistence):
        sched = Scheduler(persistence, FakeRegistry({}), None)
        await sched.handle_job_completion("ghost")


class TestDispatchLoop:
    async def _run_loop_briefly(self, sched, seconds=0.6):
        sched.running = True
        task = asyncio.create_task(sched._main_loop())
        await asyncio.sleep(seconds)
        sched.running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_dispatches_a_queued_job_once(self, persistence):
        await seed(persistence, a=job())
        await persistence.update_job_state("a", JobState.QUEUED)
        executor = RecordingExecutor()
        sched = Scheduler(persistence, FakeRegistry({"a": job()}), executor)

        await self._run_loop_briefly(sched)

        assert executor.dispatched == [("a", 1)], (
            f"expected exactly one dispatch, got {executor.dispatched}"
        )

    async def test_dispatches_nothing_while_at_capacity(self, persistence):
        await seed(persistence, a=job())
        await persistence.update_job_state("a", JobState.QUEUED)
        executor = RecordingExecutor(capacity=True)
        sched = Scheduler(persistence, FakeRegistry({"a": job()}), executor)

        await self._run_loop_briefly(sched)

        assert executor.dispatched == []
        db = await persistence.get_all_db_jobs()
        assert db["a"]["state"] is JobState.QUEUED, "an unclaimed job must stay queued"

    async def test_passes_the_persisted_attempt_number(self, persistence):
        await seed(persistence, a=job())
        executor = RecordingExecutor()
        sched = Scheduler(persistence, FakeRegistry({"a": job()}), executor)
        await sched.enqueue_job("a", bypass_deps=True, attempt=3)

        await self._run_loop_briefly(sched)

        assert executor.dispatched == [("a", 3)]

    async def test_queued_job_without_a_definition_is_blocked(self, persistence):
        await seed(persistence, ghost=job())
        await persistence.update_job_state("ghost", JobState.QUEUED)
        executor = RecordingExecutor()
        sched = Scheduler(persistence, FakeRegistry({}), executor)

        await self._run_loop_briefly(sched, seconds=1.5)

        assert executor.dispatched == []
        db = await persistence.get_all_db_jobs()
        assert db["ghost"]["state"] is JobState.BLOCKED_UNRESOLVABLE

    async def test_loop_survives_a_persistence_error(self, persistence):
        """One bad query must not take the daemon down."""
        executor = RecordingExecutor()
        sched = Scheduler(persistence, FakeRegistry({}), executor)
        calls = []

        async def exploding_claim(eligible=None):
            calls.append(1)
            raise RuntimeError("database on fire")

        sched.persistence.claim_next_queued_job = exploding_claim

        await self._run_loop_briefly(sched, seconds=0.3)

        assert calls, "the loop should have tried at least once"

    async def test_priority_order_is_respected_across_dispatches(self, persistence):
        jobs = {}
        for name, priority in [("low", 1), ("high", 9)]:
            jobs[name] = job(priority=priority)
            await persistence.upsert_job(name, jobs[name])
            await persistence.update_job_state(name, JobState.QUEUED)
        executor = RecordingExecutor()
        sched = Scheduler(persistence, FakeRegistry(jobs), executor)

        await self._run_loop_briefly(sched)

        assert [name for name, _ in executor.dispatched] == ["high", "low"]


class TestAgingLoop:
    async def test_aging_runs_on_its_interval(self, persistence):
        await persistence.upsert_job("a", JobDefinition(command="true"))
        await persistence.update_job_state("a", JobState.QUEUED)
        sched = Scheduler(persistence, FakeRegistry({}), RecordingExecutor())
        sched.running = True

        import dag_scheduler.scheduler as module
        original = module.PRIORITY_AGING_INTERVAL
        module.PRIORITY_AGING_INTERVAL = 0.1
        try:
            task = asyncio.create_task(sched._aging_loop())
            await asyncio.sleep(0.35)
            sched.running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            module.PRIORITY_AGING_INTERVAL = original

        import aiosqlite
        async with aiosqlite.connect(persistence.db_path) as db:
            async with db.execute(
                "SELECT current_priority FROM jobs WHERE name = 'a'"
            ) as c:
                priority = (await c.fetchone())[0]
        assert priority > 1, "aging should have incremented at least once"
