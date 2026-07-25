import asyncio
import logging
import time
import uuid
from typing import Optional, Tuple

from . import metrics
from .config import (
    GRACEFUL_KILL_TIMEOUT,
    LOG_BATCH_SIZE,
    LOG_FLUSH_INTERVAL,
    MAX_CONCURRENT,
)
from .models import JobState, JobDefinition, JobRun
from .process_manager import ProcessManager
from .log_store import LogStore
from .logging_setup import for_run
from .persistence import Persistence
from .retry_engine import RetryEngine

logger = logging.getLogger(__name__)

class Executor:
    def __init__(
        self,
        persistence: Persistence,
        process_manager: ProcessManager,
        log_store: LogStore,
        retry_engine: Optional[RetryEngine] = None
    ):
        self.persistence = persistence
        self.process_manager = process_manager
        self.log_store = log_store
        self.retry_engine = retry_engine
        self.scheduler = None  # Set via set_scheduler() for fan-out callbacks
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def set_retry_engine(self, retry_engine: RetryEngine):
        self.retry_engine = retry_engine

    def set_scheduler(self, scheduler):
        """Wire the executor back to the scheduler so completed jobs
        can trigger fan-out of downstream dependents."""
        self.scheduler = scheduler

    def at_capacity(self) -> bool:
        """True when every concurrency slot is taken.

        The scheduler checks this before claiming, so it does not claim
        work it cannot start.
        """
        return self.semaphore.locked()

    async def run_job(self, job_name: str, definition: JobDefinition, attempt: int = 1):
        """
        Executes a job within the concurrency limit and enforces a timeout.

        The job is already in RUNNING when this is called: the scheduler
        claims it atomically as part of selecting it.
        """
        async with self.semaphore:
            # The job may have been cancelled while this task waited for a
            # concurrency slot.  Nothing started, so nothing is recorded.
            if await self.persistence.get_job_state(job_name) is JobState.CANCELLED:
                for_run(logger, job_name, attempt=attempt).info(
                    "Cancelled before it started; not running"
                )
                return

            run_id = str(uuid.uuid4())
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')
            log = for_run(logger, job_name, run_id, attempt)

            job_run = JobRun(
                job_name=job_name,
                run_id=run_id,
                state=JobState.RUNNING,
                start_time=start_time,
                attempt=attempt
            )

            # Record the run in DB via persistence
            await self.persistence.record_run(job_run)

            log.info(f"Starting: {definition.command}")

            try:
                exit_code, timed_out = await self._execute_shell(
                    job_name, run_id, definition.command, definition.timeout
                )

                if await self._was_cancelled(job_name):
                    await self.persistence.finalize_run(
                        run_id, JobState.CANCELLED, exit_code
                    )
                    log.info(f"Cancelled, exit code {exit_code}")
                    metrics.increment("dag_runs_total", state=JobState.CANCELLED.value)
                    return

                if timed_out:
                    log.warning(f"Timed out after {definition.timeout}s")
                    metrics.increment("dag_runs_total", state=JobState.TIMED_OUT.value)
                    await self.persistence.finalize_run(
                        run_id, JobState.TIMED_OUT, exit_code
                    )
                    await self.persistence.update_job_state(
                        job_name, JobState.TIMED_OUT
                    )
                    return

                final_state = JobState.DONE if exit_code == 0 else JobState.FAILED
                log.info(f"Finished {final_state.value}, exit code {exit_code}")
                metrics.increment("dag_runs_total", state=final_state.value)
                end_time = await self.persistence.finalize_run(run_id, final_state, exit_code)
                job_run.state = final_state
                job_run.end_time = end_time
                job_run.exit_code = exit_code

                if final_state == JobState.DONE:
                    await self.persistence.update_job_state(job_name, JobState.DONE)
                    # Fan-out: notify scheduler so dependents get unblocked
                    if self.scheduler:
                        await self.scheduler.handle_job_completion(job_name)
                else:
                    # Always mark FAILED first so the state machine is consistent
                    await self.persistence.update_job_state(job_name, JobState.FAILED)
                    # Then let retry engine decide whether to re-enqueue
                    if self.retry_engine:
                        await self.retry_engine.handle_retry(definition, job_run, exit_code)

            except Exception as e:
                log.error(f"Unexpected error: {e}", exc_info=True)
                await self.persistence.finalize_run(run_id, JobState.FAILED, -1)
                if not await self._was_cancelled(job_name):
                    await self.persistence.update_job_state(job_name, JobState.FAILED)

    async def _was_cancelled(self, job_name: str) -> bool:
        """Whether the job was cancelled out from under this run.

        A cancelled job is already in a terminal state, so writing the run's
        own outcome over it would be both wrong and an illegal transition.
        Cancellation used to be discovered only by the InvalidTransitionError
        it produced, which escaped run_job entirely.
        """
        return await self.persistence.get_job_state(job_name) is JobState.CANCELLED

    async def _execute_shell(
        self, job_name: str, run_id: str, command: str, timeout: float
    ) -> Tuple[Optional[int], bool]:
        """Spawn the job, stream its output, and enforce the timeout.

        Returns (exit_code, timed_out).

        The timeout is enforced here, where the process handle is in scope.
        It used to be enforced by wrapping this coroutine in wait_for, which
        cancelled it and ran its finally clause, deregistering the process
        before the timeout handler could look it up.  The handler therefore
        found nothing and the kill never happened.

        start_new_session puts the job in its own process group so the whole
        job, not just the shell, can be signalled.
        """
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

        await self.process_manager.register_process(job_name, run_id, process)

        try:
            streaming = asyncio.gather(
                self._stream_log(run_id, 'stdout', process.stdout),
                self._stream_log(run_id, 'stderr', process.stderr),
                process.wait(),
            )
            try:
                await asyncio.wait_for(asyncio.shield(streaming), timeout=timeout)
            except asyncio.TimeoutError:
                streaming.cancel()
                try:
                    await streaming
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                exit_code = await self.process_manager.terminate(process)
                return exit_code, True
            return process.returncode, False
        finally:
            await self.process_manager.unregister_process(run_id)

    async def _stream_log(self, run_id: str, stream_name: str, stream_reader: Optional[asyncio.StreamReader]):
        """Read a stream to EOF, writing to the log store in batches.

        Lines were written one at a time, each opening a connection and
        committing.  They are batched by size and by age so a chatty job
        does not do one transaction per line, while a slow job's output
        still appears promptly.
        """
        if not stream_reader:
            return

        buffer = []
        last_flush = time.monotonic()

        async def flush():
            nonlocal buffer, last_flush
            if buffer:
                await self.log_store.store_log_chunks(run_id, buffer)
                buffer = []
            last_flush = time.monotonic()

        try:
            while True:
                line = await stream_reader.readline()
                if not line:
                    break

                buffer.append((stream_name, line.decode('utf-8', errors='replace')))
                if (
                    len(buffer) >= LOG_BATCH_SIZE
                    or time.monotonic() - last_flush >= LOG_FLUSH_INTERVAL
                ):
                    await flush()
        finally:
            await flush()
