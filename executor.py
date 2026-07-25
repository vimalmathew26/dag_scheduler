import asyncio
import logging
import time
import uuid
from typing import Optional, Tuple

from .config import MAX_CONCURRENT, DEFAULT_TIMEOUT, GRACEFUL_KILL_TIMEOUT
from .models import JobState, JobDefinition, JobRun
from .process_manager import ProcessManager
from .log_store import LogStore
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
            run_id = str(uuid.uuid4())
            start_time = time.strftime('%Y-%m-%d %H:%M:%S')

            job_run = JobRun(
                job_name=job_name,
                run_id=run_id,
                state=JobState.RUNNING,
                start_time=start_time,
                attempt=attempt
            )
            
            # Record the run in DB via persistence
            await self.persistence.record_run(job_run)

            logger.info(f"Starting job {job_name} (run_id: {run_id}, attempt: {attempt})")
            
            try:
                # Use wait_for to enforce timeout
                exit_code = await asyncio.wait_for(
                    self._execute_shell(job_name, run_id, definition.command),
                    timeout=definition.timeout
                )
                
                final_state = JobState.DONE if exit_code == 0 else JobState.FAILED
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

            except asyncio.TimeoutError:
                logger.warning(f"Job {job_name} (run_id: {run_id}) timed out after {definition.timeout}s")
                await self._handle_timeout(job_run)
            except Exception as e:
                logger.error(f"Unexpected error executing job {job_name}: {e}")
                await self.persistence.finalize_run(run_id, JobState.FAILED, -1)
                await self.persistence.update_job_state(job_name, JobState.FAILED)

    async def _execute_shell(self, job_name: str, run_id: str, command: str) -> int:
        """
        Spawns the subprocess and streams logs.
        """
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        await self.process_manager.register_process(job_name, run_id, process)
        
        try:
            # Stream stdout and stderr concurrently
            await asyncio.gather(
                self._stream_log(run_id, 'stdout', process.stdout),
                self._stream_log(run_id, 'stderr', process.stderr)
            )
            
            return await process.wait()
        finally:
            await self.process_manager.unregister_process(run_id)

    async def _stream_log(self, run_id: str, stream_name: str, stream_reader: Optional[asyncio.StreamReader]):
        if not stream_reader:
            return
            
        while True:
            line = await stream_reader.readline()
            if not line:
                break
            
            chunk = line.decode('utf-8', errors='replace')
            await self.log_store.store_log_chunk(run_id, stream_name, chunk)

    async def _handle_timeout(self, job_run: JobRun):
        """Handle a timed-out job with graceful SIGTERM then SIGKILL."""
        process = await self.process_manager.get_process(job_run.run_id)
        if process:
            try:
                # Graceful termination: SIGTERM first (#11)
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=GRACEFUL_KILL_TIMEOUT)
                except asyncio.TimeoutError:
                    # Force kill if SIGTERM didn't work
                    process.kill()
                    await process.wait()
            except ProcessLookupError:
                pass
            except Exception:
                pass
                
        await self.persistence.finalize_run(job_run.run_id, JobState.TIMED_OUT, -1)
        await self.persistence.update_job_state(job_run.job_name, JobState.TIMED_OUT)
