# retry_engine.py - Handles job retry policies with exponential backoff and jitter

import asyncio
import logging
import random
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import JobDefinition, JobRun
    from .scheduler import Scheduler

from . import metrics
from .logging_setup import for_run

logger = logging.getLogger(__name__)


class RetryEngine:
    """Handles job retry policies based on exit codes and exponential backoff with jitter."""

    def __init__(self, scheduler: "Scheduler"):
        self.scheduler = scheduler
        self._pending: set[asyncio.Task[Any]] = set()

    @staticmethod
    def _report_failure(task: "asyncio.Task[Any]") -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"Retry task failed: {exc!r}", exc_info=exc)

    def should_retry(
        self, job_definition: "JobDefinition", job_run: "JobRun", exit_code: int
    ) -> bool:
        """
        Determine if a job should be retried based on its retry policy and exit code.

        Args:
            job_definition: The job definition containing retry policy
            job_run: The job run with attempt counter
            exit_code: The exit code from the job execution

        Returns:
            bool: True if the job should be retried, False otherwise
        """
        # Check if we've exceeded max attempts
        if job_run.attempt >= job_definition.retry.max_attempts:
            return False

        # Check if exit code is in retry policy
        return exit_code in job_definition.retry.retry_on_exit_codes

    def calculate_backoff(self, job_definition: "JobDefinition", attempt: int) -> float:
        """
        Calculate the backoff time with exponential backoff and optional jitter.

        Args:
            job_definition: The job definition containing backoff parameters
            attempt: Current attempt number (1-indexed)

        Returns:
            float: Backoff time in seconds
        """
        # Calculate base backoff: backoff_base^attempt
        backoff = job_definition.retry.backoff_base**attempt

        # Apply jitter if enabled (±20%)
        if job_definition.retry.jitter:
            jitter = backoff * 0.2  # 20% jitter
            backoff += random.uniform(-jitter, jitter)

        # Ensure backoff is non-negative
        return max(0, backoff)

    async def handle_retry(
        self,
        job_definition: "JobDefinition",
        job_run: "JobRun",
        exit_code: int,
    ) -> None:
        """
        Handle retry logic for a job run.

        Note: The job is already in FAILED state when this is called.
        If a retry is warranted we schedule a delayed re-enqueue; otherwise
        the job simply stays FAILED.
        """
        if self.should_retry(job_definition, job_run, exit_code):
            # Calculate backoff time
            backoff_time = self.calculate_backoff(job_definition, job_run.attempt)

            for_run(logger, job_run.job_name, job_run.run_id, job_run.attempt).info(
                f"Failed with exit code {exit_code}, retrying in "
                f"{backoff_time:.2f}s as attempt "
                f"{job_run.attempt + 1}/{job_definition.retry.max_attempts}"
            )

            # Schedule the retry after backoff. The task is held and its
            # failures reported, rather than being fire-and-forget.
            metrics.increment("dag_retries_total")
            task = asyncio.create_task(
                self._retry_after_delay(job_run.job_name, backoff_time, attempt=job_run.attempt + 1)
            )
            self._pending.add(task)
            task.add_done_callback(self._pending.discard)
            task.add_done_callback(self._report_failure)
        else:
            for_run(logger, job_run.job_name, job_run.run_id, job_run.attempt).info(
                f"Failed with exit code {exit_code}, not retrying: "
                f"attempt {job_run.attempt} of "
                f"{job_definition.retry.max_attempts}, retry_on_exit_codes="
                f"{job_definition.retry.retry_on_exit_codes}"
            )

    async def _retry_after_delay(self, job_name: str, delay: float, attempt: int = 1) -> None:
        """
        Wait for the specified delay and then re-queue the job.

        Args:
            job_name: Name of the job to retry
            delay: Delay in seconds before retrying
            attempt: The next attempt number to use
        """
        await asyncio.sleep(delay)

        # Re-queue the job via scheduler (enqueue_job handles FAILED->DEFINED->QUEUED)
        await self.scheduler.enqueue_job(job_name, bypass_deps=True, attempt=attempt)
