import asyncio
import contextlib
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .executor import Executor
    from .persistence import Persistence
    from .registry import Registry

from . import metrics
from .config import PRIORITY_AGING_INTERVAL
from .dag import get_ready_jobs
from .models import JobState

logger = logging.getLogger(__name__)


def _report_task_failure(task: "asyncio.Task[Any]") -> None:
    """Log a background task's exception through the application logger."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(f"Background task {task.get_name()} failed: {exc!r}", exc_info=exc)


class Scheduler:
    def __init__(
        self,
        persistence: "Persistence",
        registry: "Registry",
        executor: Optional["Executor"],
    ) -> None:
        self.persistence = persistence
        self.registry = registry
        self.executor = executor
        self._loop_task: asyncio.Task[Any] | None = None
        self._aging_task: asyncio.Task[Any] | None = None
        self._dispatch_tasks: set[asyncio.Task[Any]] = set()
        self.running = False
        self.idle_poll_interval = 1.0
        self.busy_poll_interval = 0.05

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> "asyncio.Task[Any]":
        """Dispatch a job, holding a reference and reporting failures.

        A bare create_task keeps no reference, so the task can be garbage
        collected mid-flight, and surfaces failures only as "Task exception
        was never retrieved" from the asyncio logger. Anyone filtering on
        the application's own loggers saw nothing at all.
        """
        task = asyncio.create_task(coro)
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        task.add_done_callback(_report_task_failure)
        return task

    async def start(self) -> None:
        self.running = True
        self._loop_task = asyncio.create_task(self._main_loop())
        self._aging_task = asyncio.create_task(self._aging_loop())
        logger.info("Scheduler started.")

    def dispatch_tasks(self) -> list["asyncio.Task[Any]"]:
        """In-flight job tasks, so shutdown can wait for them to finalize."""
        return list(self._dispatch_tasks)

    async def stop(self) -> None:
        """Stop claiming work and wait for the loops to actually exit.

        These tasks used to be cancelled without being awaited, so the
        caller carried on while they were still unwinding.
        """
        self.running = False
        for task in (self._loop_task, self._aging_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._loop_task, self._aging_task):
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        logger.info("Scheduler stopped")

    async def _main_loop(self) -> None:
        """Claim queued jobs one at a time and dispatch them.

        Two rules keep this correct.  A job is claimed atomically, so it
        can be handed out exactly once however fast the loop spins.  And
        the loop refuses to claim while the executor is at capacity, so
        the pending task list cannot grow without bound.

        This loop used to SELECT a name, dispatch it, and immediately loop
        again with no sleep.  The row stayed QUEUED until a dispatched task
        transitioned it, so the same job was selected and dispatched
        repeatedly.  One trigger produced 22 executions.
        """
        while self.running:
            try:
                assert self.executor is not None
                if self.executor.at_capacity():
                    await asyncio.sleep(self.busy_poll_interval)
                    continue

                # One consistent snapshot per iteration, taken under the
                # reload lock, so eligibility and the definition we dispatch
                # come from the same view of the namespace.
                snapshot = await self.registry.snapshot()
                eligible = set(snapshot)
                job_name = await self.persistence.claim_next_queued_job(eligible)

                if job_name is None:
                    blocked = await self.persistence.block_queued_jobs_without_definitions(eligible)
                    for name in blocked:
                        logger.warning(
                            f"Job '{name}' is queued but has no definition; "
                            f"marking blocked_unresolvable"
                        )
                    await asyncio.sleep(self.idle_poll_interval)
                    continue

                definition = snapshot.get(job_name)
                if definition is None:
                    # Removed between the claim filter and this lookup.
                    logger.warning(f"Job '{job_name}' lost its definition during dispatch")
                    await self.persistence.update_job_state(job_name, JobState.UNKNOWN)
                    continue

                attempt = await self.persistence.get_job_attempt(job_name)
                logger.info(f"Dispatching job '{job_name}' (attempt {attempt})")
                metrics.increment("dag_dispatches_total")
                self._spawn(self.executor.run_job(job_name, definition, attempt=attempt))
            except asyncio.CancelledError:
                break
            except Exception as e:
                metrics.increment("dag_scheduler_loop_errors_total")
                logger.error(f"Error in scheduler main loop: {e}")
                await asyncio.sleep(5)

    async def enqueue_job(self, job_name: str, bypass_deps: bool = False, attempt: int = 1) -> None:
        """
        Explicitly move a job to QUEUED or WAITING.
        If bypass_deps=True, it goes straight to QUEUED.
        If False, it checks if deps are met.

        Jobs in terminal states (DONE, FAILED, TIMED_OUT, UNKNOWN, CANCELLED)
        are first reset to DEFINED so that the normal transition is valid.
        """
        definition = self.registry.get_job(job_name)
        if not definition:
            await self.persistence.update_job_state(job_name, JobState.BLOCKED_UNRESOLVABLE)
            return

        # If the job is in a terminal state, reset to DEFINED first so the
        # normal forward transition is legal.
        db_jobs = await self.persistence.get_all_db_jobs()
        current_state = db_jobs.get(job_name, {}).get("state")
        terminal_states = {
            JobState.DONE,
            JobState.FAILED,
            JobState.TIMED_OUT,
            JobState.UNKNOWN,
            JobState.CANCELLED,
        }
        if current_state in terminal_states:
            await self.persistence.reset_job_state(job_name)

        # Record which attempt this dispatch represents.  This parameter
        # used to be accepted and silently dropped, so every retry was
        # dispatched as attempt 1 and the retry limit was never reached.
        await self.persistence.set_job_attempt(job_name, attempt)

        if bypass_deps:
            await self.persistence.update_job_state(job_name, JobState.QUEUED)
            return

        # Check dependencies
        db_jobs = await self.persistence.get_all_db_jobs()
        current_states = {name: info["state"] for name, info in db_jobs.items()}

        ready = True
        for dep in definition.depends_on:
            if dep not in current_states or current_states[dep] != JobState.DONE:
                ready = False
                break

        target_state = JobState.QUEUED if ready else JobState.WAITING
        await self.persistence.update_job_state(job_name, target_state)

    async def handle_job_completion(self, job_name: str) -> None:
        """
        Called when a job finishes (DONE). Unblocks dependents.
        """
        definition = self.registry.get_job(job_name)
        if not definition:
            return

        db_jobs = await self.persistence.get_all_db_jobs()
        current_states = {name: info["state"] for name, info in db_jobs.items()}

        # registry.get_all_jobs() gives the full snapshot for dag fan-out
        jobs_snapshot = self.registry.get_all_jobs()

        newly_ready = get_ready_jobs(job_name, jobs_snapshot, current_states)
        for next_job in newly_ready:
            # Transition waiting -> queued
            try:
                await self.persistence.update_job_state(next_job, JobState.QUEUED)
                logger.info(f"Dependency met: {job_name} -> {next_job}. Queued {next_job}.")
            except Exception as e:
                logger.error(f"Failed to unblock {next_job}: {e}")

    async def _aging_loop(self) -> None:
        """Increments priority of queued jobs every PRIORITY_AGING_INTERVAL seconds."""
        while self.running:
            await asyncio.sleep(PRIORITY_AGING_INTERVAL)
            try:
                await self.persistence.age_queued_priorities()
                logger.debug("Priority aging applied to queued jobs.")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in priority aging loop: {e}")
