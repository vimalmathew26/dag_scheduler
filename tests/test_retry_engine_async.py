"""RetryEngine's scheduling half, the file watcher's debounce, and log reads."""

import asyncio

from dag_scheduler.file_watcher import FileWatcher
from dag_scheduler.models import JobDefinition, JobRun, JobState, RetryPolicy
from dag_scheduler.retry_engine import RetryEngine


class RecordingScheduler:
    def __init__(self):
        self.enqueued = []

    async def enqueue_job(self, name, bypass_deps=False, attempt=1):
        self.enqueued.append((name, bypass_deps, attempt))


def definition(**policy):
    return JobDefinition(command="exit 1", retry=RetryPolicy(**policy))


def run(attempt):
    return JobRun(job_name="j", run_id="r", state=JobState.FAILED, attempt=attempt)


class TestHandleRetry:
    async def test_eligible_failure_is_re_enqueued_with_the_next_attempt(self):
        sched = RecordingScheduler()
        engine = RetryEngine(sched)

        await engine.handle_retry(
            definition(max_attempts=3, backoff_base=1.0, jitter=False), run(1), 1
        )
        await asyncio.sleep(1.4)

        assert sched.enqueued == [("j", True, 2)]

    async def test_exhausted_attempts_are_not_re_enqueued(self):
        sched = RecordingScheduler()
        engine = RetryEngine(sched)

        await engine.handle_retry(
            definition(max_attempts=3, backoff_base=1.0, jitter=False), run(3), 1
        )
        await asyncio.sleep(1.4)

        assert sched.enqueued == []

    async def test_exit_code_outside_the_policy_is_not_re_enqueued(self):
        sched = RecordingScheduler()
        engine = RetryEngine(sched)

        await engine.handle_retry(
            definition(max_attempts=3, retry_on_exit_codes=[1], backoff_base=1.0, jitter=False),
            run(1),
            42,
        )
        await asyncio.sleep(1.4)

        assert sched.enqueued == []

    async def test_backoff_is_actually_waited(self):
        sched = RecordingScheduler()
        engine = RetryEngine(sched)

        await engine.handle_retry(
            definition(max_attempts=3, backoff_base=2.0, jitter=False), run(1), 1
        )
        await asyncio.sleep(0.5)
        assert sched.enqueued == [], "re-enqueued before the backoff elapsed"

        await asyncio.sleep(2.0)
        assert sched.enqueued == [("j", True, 2)]


class TestFileWatcherDebounce:
    async def test_rapid_events_coalesce_into_one_reload(self, tmp_path):
        reloads = []

        class FakeRegistry:
            async def reload(self):
                reloads.append(1)

        watcher = FileWatcher(FakeRegistry(), tmp_path, debounce_delay=0.2)

        class Event:
            def __init__(self, path):
                self.src_path = str(path)
                self.is_directory = False

        target = tmp_path / "a.yaml"
        for _ in range(5):
            await watcher._handle_event(Event(target))
            await asyncio.sleep(0.02)

        await asyncio.sleep(0.5)
        # Documents current behaviour, which does not match the README's
        # claim that edits are coalesced. See FINDINGS.md F5: the debounce
        # task's finally clause pops whichever task is registered for the
        # path rather than itself, so it deregisters its own replacement and
        # several reloads fire. Five rapid edits produce three reloads.
        assert len(reloads) > 1, (
            "if this now passes with one reload, F5 has been fixed and this "
            "test should assert coalescing instead"
        )

    async def test_non_definition_files_are_ignored(self, tmp_path):
        reloads = []

        class FakeRegistry:
            async def reload(self):
                reloads.append(1)

        watcher = FileWatcher(FakeRegistry(), tmp_path, debounce_delay=0.05)

        class Event:
            def __init__(self, path):
                self.src_path = str(path)
                self.is_directory = False

        await watcher._handle_event(Event(tmp_path / "notes.txt"))
        await asyncio.sleep(0.2)
        assert reloads == []

    async def test_a_failing_reload_does_not_escape(self, tmp_path):
        class ExplodingRegistry:
            async def reload(self):
                raise RuntimeError("bad definition file")

        watcher = FileWatcher(ExplodingRegistry(), tmp_path, debounce_delay=0.05)

        class Event:
            src_path = str(tmp_path / "a.yaml")
            is_directory = False

        await watcher._handle_event(Event())
        await asyncio.sleep(0.3)
        # Reaching here without an unhandled exception is the assertion.


class TestLogStore:
    async def test_logs_for_job_returns_the_most_recent_run(self, persistence, tmp_path):
        from dag_scheduler.log_store import LogStore

        store = LogStore(persistence.db_path)
        await persistence.upsert_job("j", JobDefinition(command="true"))
        for run_id, ts in [("old", "2026-01-01 00:00:00"), ("new", "2026-01-02 00:00:00")]:
            await persistence.record_run(
                JobRun(job_name="j", run_id=run_id, state=JobState.RUNNING, start_time=ts)
            )
            await store.store_log_chunk(run_id, "stdout", f"from {run_id}\n")

        entries = await store.get_logs_for_job("j")

        assert entries and all("from new" in chunk for _, chunk, _ in entries)

    async def test_no_runs_yields_no_logs(self, persistence):
        from dag_scheduler.log_store import LogStore

        assert await LogStore(persistence.db_path).get_logs_for_job("nope") == []
