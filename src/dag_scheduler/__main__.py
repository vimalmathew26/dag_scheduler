import asyncio
import signal
import logging
import sys
import uvicorn
from .config import (
    API_HOST,
    API_PORT,
    DB_PATH,
    JOBS_DIR,
    LOG_JSON,
    LOG_LEVEL,
    SHUTDOWN_TIMEOUT,
    ensure_directories,
)
from .persistence import Persistence
from .registry import Registry
from .log_store import LogStore
from .process_manager import ProcessManager
from .executor import Executor
from .scheduler import Scheduler
from .file_watcher import FileWatcher
from .retry_engine import RetryEngine
from .api import app, init_api
from .logging_setup import configure_logging

configure_logging(LOG_LEVEL, json_output=LOG_JSON)
logger = logging.getLogger("dag_scheduler.main")

class Daemon:
    def __init__(self, db_path=None, jobs_dir=None):
        self.db_path = db_path or DB_PATH
        self.jobs_dir = jobs_dir or JOBS_DIR
        ensure_directories(self.db_path, self.jobs_dir)

        self.stop_event = asyncio.Event()
        self.persistence = Persistence(self.db_path)
        self.log_store = LogStore(self.db_path)
        self.process_manager = ProcessManager(self.persistence)
        self.registry = Registry(self.persistence, self.jobs_dir)

        # Executor initialized with placeholders, retry_engine linked later
        self.executor = Executor(self.persistence, self.process_manager, self.log_store)
        self.scheduler = Scheduler(self.persistence, self.registry, self.executor)
        self.retry_engine = RetryEngine(self.scheduler)
        self.executor.set_retry_engine(self.retry_engine)
        self.executor.set_scheduler(self.scheduler)

        self.file_watcher = FileWatcher(self.registry, self.jobs_dir)

        self.api_server = None
        self.api_task = None
        self._shutting_down = False

    async def shutdown(self, sig=None):
        """Stop cleanly: drain the API, kill jobs, let writes finish.

        The old sequence set a stop event and then cancelled every
        outstanding task. That abandoned running subprocesses, which
        outlived the daemon; it interrupted run_job between record_run and
        finalize_run, corrupting run history on every clean stop the same
        way a crash did; and it cancelled uvicorn rather than asking it to
        exit, which printed a CancelledError traceback at ERROR level on
        every single shutdown.
        """
        if self._shutting_down:
            return
        self._shutting_down = True

        if sig:
            logger.info(f"Received exit signal {sig.name}, shutting down")

        # 1. Stop claiming new work, so nothing new starts while we drain.
        logger.info("Stopping scheduler")
        await self.scheduler.stop()

        # 2. Stop watching for definition changes.
        logger.info("Stopping file watcher")
        await self.file_watcher.stop()

        # 3. Ask the API to drain rather than cancelling it mid-request.
        if self.api_server is not None:
            logger.info("Draining API server")
            self.api_server.should_exit = True
            if self.api_task is not None:
                try:
                    await asyncio.wait_for(self.api_task, timeout=SHUTDOWN_TIMEOUT)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    self.api_task.cancel()

        # 4. Terminate running jobs, then give their tasks a moment to
        #    write their own final state. A job killed here exits non-zero
        #    and is recorded, rather than vanishing mid-write.
        killed = await self.process_manager.terminate_all()
        if killed:
            logger.info(f"Terminated {killed} running job process(es)")

        inflight = [t for t in self.scheduler.dispatch_tasks() if not t.done()]
        if inflight:
            logger.info(f"Waiting for {len(inflight)} in-flight job(s) to finalize")
            done, pending = await asyncio.wait(inflight, timeout=SHUTDOWN_TIMEOUT)
            for task in pending:
                logger.warning("A job task did not finalize in time; cancelling")
                task.cancel()

        self.stop_event.set()
        logger.info("Daemon shutdown complete")

    async def run(self):
        # 1. DB Setup
        await self.persistence.setup()

        # 2. Crash Recovery
        await self.process_manager.handle_crash_recovery()

        # 3. Registry Initial Load
        try:
            await self.registry.load_initial()
        except Exception as e:
            logger.error(f"Failed to load initial job definitions: {e}")
            # We continue even if initial load fails, so user can fix files later

        # 4. Initialize API dependencies
        init_api(self.scheduler, self.registry, self.persistence, self.log_store, self.process_manager)

        # 5. Start Scheduler and File Watcher
        await self.scheduler.start()
        self.watcher_task = asyncio.create_task(self.file_watcher.start())

        # 6. Start API Server (Uvicorn)
        config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="info")
        self.api_server = uvicorn.Server(config)
        # Uvicorn installs its own signal handlers by default, which would
        # race ours. The daemon owns the shutdown sequence.
        self.api_server.install_signal_handlers = lambda: None
        self.api_task = asyncio.create_task(self.api_server.serve())

        logger.info(f"Daemon started (API: {API_HOST}:{API_PORT})")

        # Wait for stop event
        await self.stop_event.wait()

async def main_async():
    daemon = Daemon()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(daemon.shutdown(s)))

    try:
        await daemon.run()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"Fatal error in daemon: {e}")
        sys.exit(1)

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
