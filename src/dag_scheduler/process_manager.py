# process_manager.py - Tracks active subprocesses and handles 'unknown' state detection

import asyncio
import contextlib
import logging
import os
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .persistence import Persistence
from .config import GRACEFUL_KILL_TIMEOUT
from .models import JobState

logger = logging.getLogger(__name__)


class ProcessManager:
    """Tracks live subprocess handles and manages crash recovery state transitions."""

    def __init__(self, persistence: "Persistence") -> None:
        self.persistence = persistence
        self.processes: dict[str, asyncio.subprocess.Process] = {}  # run_id -> process
        self.job_to_run: dict[str, str] = {}  # job_name -> current run_id
        self._lock = asyncio.Lock()

    async def register_process(
        self, job_name: str, run_id: str, process: asyncio.subprocess.Process
    ) -> None:
        """Register a running subprocess."""
        async with self._lock:
            self.processes[run_id] = process
            self.job_to_run[job_name] = run_id

    async def unregister_process(self, run_id: str) -> None:
        """Unregister a completed subprocess."""
        async with self._lock:
            self.processes.pop(run_id, None)
            # Clean up reverse mapping
            self.job_to_run = {k: v for k, v in self.job_to_run.items() if v != run_id}

    async def get_process(self, job_run_id: str) -> asyncio.subprocess.Process | None:
        """Get a registered subprocess by job_run_id."""
        async with self._lock:
            return self.processes.get(job_run_id)

    async def handle_crash_recovery(self) -> None:
        """On daemon startup, mark all RUNNING jobs as UNKNOWN.

        At startup no subprocesses are tracked, so any job still recorded as
        RUNNING in the database is orphaned from a previous daemon lifetime.
        """
        logger.info("Starting crash recovery process detection")
        db_jobs = await self.persistence.get_all_db_jobs()

        unknown_jobs: set[str] = set()
        for job_name, info in db_jobs.items():
            if info["state"] == JobState.RUNNING:
                # No live process exists after restart — mark as unknown
                unknown_jobs.add(job_name)

        for job_name in unknown_jobs:
            logger.warning(f"Marking job '{job_name}' as unknown after crash recovery")
            try:
                await self.persistence.update_job_state(job_name, JobState.UNKNOWN)
            except Exception as e:
                logger.error(f"Failed to mark job '{job_name}' as unknown: {e}")

        # Run rows are reconciled too.  Recovery used to touch only the
        # jobs table, leaving orphaned runs claiming to be executing
        # forever and permanently understating the pass rate in /stats.
        orphaned_runs = await self.persistence.finalize_orphaned_runs()
        if orphaned_runs:
            logger.warning(f"Finalized {orphaned_runs} orphaned job run(s) as unknown")

        logger.info(
            f"Crash recovery completed. Marked {len(unknown_jobs)} jobs and "
            f"{orphaned_runs} run(s) as unknown."
        )

    async def terminate(self, process: asyncio.subprocess.Process) -> int | None:
        """Kill a job: SIGTERM the process group, then SIGKILL if needed.

        A job command runs under a shell, so the direct child is usually
        `/bin/sh -c ...` and the real work is a grandchild.  Signalling only
        the child leaves the grandchild orphaned, which is what timed-out
        jobs used to do.  Processes are spawned in their own session so the
        whole group can be signalled at once.

        Returns the process return code, or None if it could not be reaped.
        """
        if process.returncode is not None:
            return process.returncode

        self._signal_group(process, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=GRACEFUL_KILL_TIMEOUT)
            return process.returncode
        except asyncio.TimeoutError:
            logger.warning(
                f"Process {process.pid} ignored SIGTERM after "
                f"{GRACEFUL_KILL_TIMEOUT}s; sending SIGKILL"
            )

        self._signal_group(process, signal.SIGKILL)
        try:
            await asyncio.wait_for(process.wait(), timeout=GRACEFUL_KILL_TIMEOUT)
        except asyncio.TimeoutError:
            logger.error(f"Process {process.pid} survived SIGKILL")
        return process.returncode

    @staticmethod
    def _signal_group(process: asyncio.subprocess.Process, sig: int) -> None:
        """Signal the process group, falling back to the direct child."""
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            os.killpg(os.getpgid(process.pid), sig)
            return
        with contextlib.suppress(ProcessLookupError, OSError):
            process.send_signal(sig)

    async def terminate_all(self) -> int:
        """Terminate every tracked process. Used on shutdown.

        Returns how many were signalled.  Shutdown used to cancel the tasks
        supervising these processes without touching the processes
        themselves, so they outlived the daemon.
        """
        async with self._lock:
            processes = list(self.processes.values())

        for process in processes:
            try:
                await self.terminate(process)
            except Exception as e:
                logger.error(f"Error terminating process {process.pid}: {e}")
        return len(processes)

    async def kill_by_job_name(self, job_name: str) -> bool:
        """Gracefully terminate (SIGTERM then SIGKILL) the process for a job.

        Returns True if a process was found and killed, False otherwise.
        """
        async with self._lock:
            run_id = self.job_to_run.get(job_name)
            if not run_id:
                return False
            process = self.processes.get(run_id)
            if not process:
                return False

        # Kill outside lock to avoid holding it during I/O
        try:
            await self.terminate(process)
            return True
        except Exception as e:
            logger.error(f"Error killing process for job '{job_name}': {e}")
            return False
