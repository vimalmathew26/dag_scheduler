import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from . import metrics
from .config import JOBS_DIR

logger = logging.getLogger(__name__)

class FileWatcher:
    def __init__(
        self,
        registry: Any,
        jobs_dir: Optional[Path] = None,
        debounce_delay: float = 0.5,
    ) -> None:
        self.registry = registry
        self.jobs_dir = Path(jobs_dir) if jobs_dir is not None else JOBS_DIR
        self.debounce_delay = debounce_delay
        self.event_queue: 'asyncio.Queue[Any]' = asyncio.Queue()
        self._observer = Observer()
        self._handler = _FileSystemHandler(self.event_queue)
        self._watched_dirs: Set[Path] = set()
        self._debounce_tasks: Dict[Path, 'asyncio.Task[Any]'] = {}
        self._loop: Optional[Any] = None

    async def start(self) -> None:
        """Start watching job definition directories."""
        # Capture the running event loop
        self._loop = asyncio.get_event_loop()

        # Watch the main jobs directory
        if self.jobs_dir.exists():
            self._observer.schedule(self._handler, str(self.jobs_dir), recursive=False)
            self._watched_dirs.add(self.jobs_dir)
            logger.info(f"Started watching {self.jobs_dir}")

        self._observer.start()
        logger.info("File watcher started")

        # Pass the loop to the handler
        self._handler.set_loop(self._loop)

        # Process events from queue
        while True:
            try:
                event = await self.event_queue.get()
                await self._handle_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing file event: {e}")

    async def stop(self) -> None:
        """Stop watching directories.

        observer.join() is a blocking thread join, so it runs in an executor
        rather than stalling the event loop during shutdown.
        """
        self._observer.stop()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._observer.join)
        logger.info("File watcher stopped")

    async def _handle_event(self, event: Any) -> None:
        """Handle a file system event with debouncing."""
        src_path = Path(event.src_path)

        # Filter for YAML/TOML files
        if src_path.suffix not in ['.yaml', '.yml', '.toml']:
            return

        # Cancel any existing debounce task for this file
        if src_path in self._debounce_tasks:
            self._debounce_tasks[src_path].cancel()

        # Create new debounce task
        task = asyncio.create_task(self._debounce_reload(src_path))
        self._debounce_tasks[src_path] = task
        task.add_done_callback(self._report_failure)

    @staticmethod
    def _report_failure(task: Any) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"Reload task failed: {exc!r}", exc_info=exc)

    async def _debounce_reload(self, file_path: Path) -> None:
        """Reload registry after debounce delay."""
        try:
            await asyncio.sleep(self.debounce_delay)
            logger.info(f"Reloading registry due to change in {file_path}")
            await self.registry.reload()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            metrics.increment("dag_reload_failures_total")
            logger.error(f"Error during registry reload: {e}")
        finally:
            # Clean up the task reference
            self._debounce_tasks.pop(file_path, None)


class _FileSystemHandler(FileSystemEventHandler):
    """Internal handler for file system events."""

    def __init__(self, event_queue: 'asyncio.Queue[Any]') -> None:
        self.event_queue = event_queue
        self._loop: Optional[Any] = None

    def set_loop(self, loop: Any) -> None:
        """Set the asyncio event loop for thread-safe operations."""
        self._loop = loop

    def on_modified(self, event: Any) -> None:
        if not event.is_directory:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)

    def on_created(self, event: Any) -> None:
        if not event.is_directory:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)

    def on_deleted(self, event: Any) -> None:
        if not event.is_directory:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)
