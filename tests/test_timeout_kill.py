"""B3: a timed-out job must have its processes killed.

asyncio.wait_for cancelled _execute_shell, whose finally deregistered the
process before TimeoutError propagated. _handle_timeout then looked the
process up, got None, and the SIGTERM/SIGKILL block never ran. Observed:
4 'sleep 30' processes alive 8 seconds after a 2 second timeout, and still
alive after the daemon exited.

A shell command also spawns children. Killing only the direct child leaves
`sleep 30` running under a dead `/bin/sh`, so the job is signalled as a
process group.
"""

import asyncio
import os

import pytest

from dag_scheduler.executor import Executor
from dag_scheduler.log_store import LogStore
from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.process_manager import ProcessManager
from tests.conftest import requires_posix


def group_members(pgid: int) -> set:
    """PIDs currently in a process group, read from /proc.

    Matching on command line is unreliable here: the surrounding test
    harness has the job's command text in its own argv. Process group
    membership is exact.
    """
    members = set()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                fields = fh.read().rsplit(")", 1)[1].split()
            if int(fields[2]) == pgid:
                members.add(int(entry))
        except (OSError, IndexError, ValueError):
            continue
    return members


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


async def wait_gone(pid: int, timeout: float = 10.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if not alive(pid):
            return True
        await asyncio.sleep(0.1)
    return not alive(pid)


@pytest.fixture
def executor(persistence, tmp_path):
    pm = ProcessManager(persistence)
    return Executor(persistence, pm, LogStore(tmp_path / "test.db"))


async def run(executor, persistence, name, command, timeout=2):
    definition = JobDefinition(command=command, timeout=timeout)
    await persistence.upsert_job(name, definition)
    await persistence.update_job_state(name, JobState.QUEUED)
    await persistence.claim_next_queued_job()
    await executor.run_job(name, definition)
    return definition


class TestTimeoutKillsTheProcess:
    @requires_posix
    async def test_job_is_marked_timed_out(self, executor, persistence):
        await run(executor, persistence, "slow", "sleep 30", timeout=1)
        jobs = await persistence.get_all_db_jobs()
        assert jobs["slow"]["state"] is JobState.TIMED_OUT

    @requires_posix
    async def test_direct_child_is_dead_after_timeout(self, executor, persistence):
        pids = []

        original = asyncio.create_subprocess_shell

        async def capture(*args, **kwargs):
            proc = await original(*args, **kwargs)
            pids.append(proc.pid)
            return proc

        asyncio.create_subprocess_shell = capture
        try:
            await run(executor, persistence, "slow", "sleep 30", timeout=1)
        finally:
            asyncio.create_subprocess_shell = original

        assert pids, "no subprocess was spawned"
        assert await wait_gone(pids[0]), "the shell survived its own timeout"

    async def _run_and_collect_group(self, executor, persistence, name, command):
        """Run a job to timeout, returning the PIDs that were in its group."""
        seen = {}
        original = asyncio.create_subprocess_shell

        async def capture(*args, **kwargs):
            proc = await original(*args, **kwargs)
            seen["proc"] = proc
            # Let the shell fork its child before we snapshot the group.
            await asyncio.sleep(0.4)
            seen["members"] = group_members(os.getpgid(proc.pid))
            return proc

        asyncio.create_subprocess_shell = capture
        try:
            await run(executor, persistence, name, command, timeout=1)
        finally:
            asyncio.create_subprocess_shell = original
        return seen["members"]

    @requires_posix
    async def test_whole_process_group_is_dead_after_timeout(self, executor, persistence):
        """The regression test for the orphans seen in production.

        `sh -c "sleep 30"` leaves a `sleep` grandchild. Signalling only the
        direct child orphans it, which is what the before-state showed: both
        `/bin/sh -c sleep 30` and `sleep 30` outlived the daemon.
        """
        members = await self._run_and_collect_group(executor, persistence, "slow", "sleep 30")
        assert len(members) >= 2, f"expected a shell and a grandchild, saw {members}"
        await asyncio.sleep(0.5)
        survivors = {pid for pid in members if alive(pid)}
        assert not survivors, f"orphaned processes survived the timeout: {survivors}"

    @requires_posix
    async def test_process_ignoring_sigterm_is_sigkilled(self, executor, persistence, monkeypatch):
        """Exercises the SIGKILL escalation, which had never executed."""
        monkeypatch.setattr("dag_scheduler.process_manager.GRACEFUL_KILL_TIMEOUT", 1)
        members = await self._run_and_collect_group(
            executor, persistence, "stubborn", "trap '' TERM; sleep 30"
        )
        await asyncio.sleep(0.5)
        survivors = {pid for pid in members if alive(pid)}
        assert not survivors, f"processes ignoring SIGTERM were never SIGKILLed: {survivors}"

    async def test_normal_completion_is_unaffected(self, executor, persistence):
        await run(executor, persistence, "quick", "echo hello", timeout=10)
        jobs = await persistence.get_all_db_jobs()
        assert jobs["quick"]["state"] is JobState.DONE

    @requires_posix
    async def test_timed_out_run_records_end_time(self, executor, persistence):
        await run(executor, persistence, "slow", "sleep 30", timeout=1)
        runs = await persistence.get_runs_for_job("slow")
        assert len(runs) == 1
        assert runs[0]["state"] == JobState.TIMED_OUT.value
        assert runs[0]["end_time"] is not None
