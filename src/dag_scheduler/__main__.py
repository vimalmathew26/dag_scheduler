import asyncio
import signal
import logging
import sys
import uvicorn
from .config import DB_PATH, API_PORT, API_HOST, JOBS_DIR, ensure_directories
from .persistence import Persistence
from .registry import Registry
from .log_store import LogStore
from .process_manager import ProcessManager
from .executor import Executor
from .scheduler import Scheduler
from .file_watcher import FileWatcher
from .retry_engine import RetryEngine
from .api import app, init_api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
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

    async def shutdown(self, sig=None):
        if sig:
            logger.info(f"Received exit signal {sig.name}...")
        self.stop_event.set()

        logger.info("Stopping scheduler...")
        await self.scheduler.stop()

        logger.info("Stopping file watcher...")
        await self.file_watcher.stop()

        # API (uvicorn) shutdown is handled by the server task being cancelled

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [t.cancel() for t in tasks]

        logger.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Daemon shutdown complete.")

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
        scheduler_task = asyncio.create_task(self.scheduler.start())
        watcher_task = asyncio.create_task(self.file_watcher.start())

        # 6. Start API Server (Uvicorn)
        config = uvicorn.Config(app, host=API_HOST, port=API_PORT, log_level="info")
        server = uvicorn.Server(config)
        api_task = asyncio.create_task(server.serve())

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
