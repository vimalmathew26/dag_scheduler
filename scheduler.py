import asyncio
import logging
from typing import Dict, List, Optional, Set

from .config import MAX_CONCURRENT, PRIORITY_AGING_INTERVAL
from .models import JobState, JobDefinition
from .dag import get_ready_jobs

logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self, persistence, registry, executor):
        self.persistence = persistence
        self.registry = registry
        self.executor = executor
        self._loop_task: Optional[asyncio.Task] = None
        self._aging_task: Optional[asyncio.Task] = None
        self._dispatch_tasks: Set[asyncio.Task] = set()
        self.running = False
        self.idle_poll_interval = 1.0
        self.busy_poll_interval = 0.05

    def _spawn(self, coro) -> asyncio.Task:
        """Dispatch a job, holding a reference so it is not collected."""
        task = asyncio.create_task(coro)
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        return task

    async def start(self):
        self.running = True
        self._loop_task = asyncio.create_task(self._main_loop())
        self._aging_task = asyncio.create_task(self._aging_loop())
        logger.info("Scheduler started.")

    async def stop(self):
        self.running = False
        if self._loop_task:
            self._loop_task.cancel()
        if self._aging_task:
            self._aging_task.cancel()
        logger.info("Scheduler stopped.")

    async def _main_loop(self):
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
                if self.executor.at_capacity():
                    await asyncio.sleep(self.busy_poll_interval)
                    continue

                eligible = self.registry.known_job_names()
                job_name = await self.persistence.claim_next_queued_job(eligible)

                if job_name is None:
                    blocked = await self.persistence.block_queued_jobs_without_definitions(
                        eligible
                    )
                    for name in blocked:
                        logger.warning(
                            f"Job '{name}' is queued but has no definition; "
                            f"marking blocked_unresolvable"
                        )
                    await asyncio.sleep(self.idle_poll_interval)
                    continue

                definition = self.registry.get_job(job_name)
                if definition is None:
                    # Removed between the claim filter and this lookup.
                    logger.warning(
                        f"Job '{job_name}' lost its definition during dispatch"
                    )
                    await self.persistence.update_job_state(job_name, JobState.UNKNOWN)
                    continue

                attempt = await self.persistence.get_job_attempt(job_name)
                logger.info(f"Dispatching job '{job_name}' (attempt {attempt})")
                self._spawn(
                    self.executor.run_job(job_name, definition, attempt=attempt)
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler main loop: {e}")
                await asyncio.sleep(5)

    async def enqueue_job(self, job_name: str, bypass_deps: bool = False, attempt: int = 1):
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
        current_state = db_jobs.get(job_name, {}).get('state')
        terminal_states = {
            JobState.DONE, JobState.FAILED, JobState.TIMED_OUT,
            JobState.UNKNOWN, JobState.CANCELLED,
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
        current_states = {name: info['state'] for name, info in db_jobs.items()}
        
        ready = True
        for dep in definition.depends_on:
            if dep not in current_states or current_states[dep] != JobState.DONE:
                ready = False
                break
        
        target_state = JobState.QUEUED if ready else JobState.WAITING
        await self.persistence.update_job_state(job_name, target_state)

    async def handle_job_completion(self, job_name: str):
        """
        Called when a job finishes (DONE). Unblocks dependents.
        """
        definition = self.registry.get_job(job_name)
        if not definition: return

        db_jobs = await self.persistence.get_all_db_jobs()
        current_states = {name: info['state'] for name, info in db_jobs.items()}
        
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

    async def _aging_loop(self):
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