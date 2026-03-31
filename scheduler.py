import asyncio
import logging
from typing import Dict, List, Optional

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
        self.running = False

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
        """Polls for queued jobs and dispatches them."""
        while self.running:
            try:
                # Respect MAX_CONCURRENT via executor's semaphore is handled inside executor.run_job,
                # but we can also check here to avoid over-polling if we're full.
                # However, the spec says 'Dispatches queued jobs to executor respecting MAX_CONCURRENT semaphore'.
                # We will pick the highest priority QUEUED job and start it.
                
                job_name = await self._get_next_queued_job()
                if job_name:
                    definition = self.registry.get_job(job_name)
                    if definition:
                        # Fire and forget - the executor handles semaphore and state updates
                        asyncio.create_task(self.executor.run_job(job_name, definition))
                    else:
                        # Job in DB but not in registry? Mark blocked.
                        await self.persistence.update_job_state(job_name, JobState.BLOCKED_UNRESOLVABLE)
                else:
                    await asyncio.sleep(1) # Wait for jobs to be queued
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler main loop: {e}")
                await asyncio.sleep(5)

    async def _get_next_queued_job(self) -> Optional[str]:
        """Fetch the highest priority job that is currently QUEUED (via persistence)."""
        return await self.persistence.get_next_queued_job()

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

        # If the job is in a terminal state, reset to DEFINED first (#2, #8)
        db_jobs = await self.persistence.get_all_db_jobs()
        current_state = db_jobs.get(job_name, {}).get('state')
        terminal_states = {
            JobState.DONE, JobState.FAILED, JobState.TIMED_OUT,
            JobState.UNKNOWN, JobState.CANCELLED,
        }
        if current_state in terminal_states:
            await self.persistence.reset_job_state(job_name)

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