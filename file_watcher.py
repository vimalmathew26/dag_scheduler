import asyncio
import logging
from pathlib import Path
from typing import Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .config import JOBS_DIR

logger = logging.getLogger(__name__)

class FileWatcher:
    def __init__(self, registry, debounce_delay: float = 0.5):
        self.registry = registry
        self.debounce_delay = debounce_delay
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self._observer = Observer()
        self._handler = _FileSystemHandler(self.event_queue)
        self._watched_dirs: Set[Path] = set()
        self._debounce_tasks = {}
        self._loop = None

    async def start(self):
        """Start watching job definition directories."""
        # Capture the running event loop
        self._loop = asyncio.get_event_loop()
        
        # Watch the main jobs directory
        if JOBS_DIR.exists():
            self._observer.schedule(self._handler, str(JOBS_DIR), recursive=False)
            self._watched_dirs.add(JOBS_DIR)
            logger.info(f"Started watching {JOBS_DIR}")
        
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
    
    async def stop(self):
        """Stop watching directories."""
        self._observer.stop()
        self._observer.join()
        logger.info("File watcher stopped")
    
    async def _handle_event(self, event):
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
    
    async def _debounce_reload(self, file_path: Path):
        """Reload registry after debounce delay."""
        try:
            await asyncio.sleep(self.debounce_delay)
            logger.info(f"Reloading registry due to change in {file_path}")
            await self.registry.reload()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error during registry reload: {e}")
        finally:
            # Clean up the task reference
            self._debounce_tasks.pop(file_path, None)


class _FileSystemHandler(FileSystemEventHandler):
    """Internal handler for file system events."""
    
    def __init__(self, event_queue: asyncio.Queue):
        self.event_queue = event_queue
        self._loop = None
    
    def set_loop(self, loop):
        """Set the asyncio event loop for thread-safe operations."""
        self._loop = loop
    
    def on_modified(self, event):
        if not event.is_directory:
            self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)
    
    def on_created(self, event):
        if not event.is_directory:
            self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)
    
    def on_deleted(self, event):
        if not event.is_directory:
            self._loop.call_soon_threadsafe(self.event_queue.put_nowait, event)
