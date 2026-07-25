import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional, Set
from .models import JobDefinition, JobState
from .definition_parser import DefinitionParser
from .config import JOBS_DIR

logger = logging.getLogger(__name__)

class Registry:
    """
    Manages the flat job namespace, handling atomic hot-reload swaps,
    diffing definitions, and transitioning removed jobs.
    """

    def __init__(self, persistence, jobs_dir: Optional[Path] = None):
        self.persistence = persistence
        self.jobs_dir = Path(jobs_dir) if jobs_dir is not None else JOBS_DIR
        self.jobs: Dict[str, JobDefinition] = {}
        self._reload_lock = asyncio.Lock()

    async def load_initial(self):
        """Initial load of jobs on startup."""
        async with self._reload_lock:
            parser = DefinitionParser()
            try:
                new_jobs = parser.parse_directory(self.jobs_dir)
                self.jobs = new_jobs
                for name, definition in new_jobs.items():
                    await self.persistence.upsert_job(name, definition)
            except Exception as e:
                logger.error(f"Initial load failed: {e}")
                # On initial load, we might want to let the daemon start anyway 
                # if some files are valid, or hard error. The spec says 
                # parse_directory raises ParseError on any violation.
                raise e

    async def reload(self):
        """
        Hot-reload job definitions. Atomic swap after validation.
        Serializes concurrent reload events.
        """
        async with self._reload_lock:
            parser = DefinitionParser()
            try:
                # parse_directory handles validation (two-pass, cycle, etc.)
                new_jobs = parser.parse_directory(self.jobs_dir)
            except Exception as e:
                logger.error(f"Reload failed: {e}")
                # Atomic swap: new snapshot only replaces old after clean validation.
                raise e

            old_jobs = self.jobs
            self.jobs = new_jobs
            await self._handle_diff(old_jobs, new_jobs)

    async def _handle_diff(self, old_jobs: Dict[str, JobDefinition], new_jobs: Dict[str, JobDefinition]):
        """
        Diffs old vs new snapshot and handles removed/changed jobs.
        """
        removed_names = set(old_jobs.keys()) - set(new_jobs.keys())
        added_names = set(new_jobs.keys()) - set(old_jobs.keys())
        changed_names = {
            name for name in set(old_jobs.keys()) & set(new_jobs.keys())
            if old_jobs[name] != new_jobs[name]
        }

        # 1. Handle Removed Jobs (transition queued→blocked_unresolvable, running→let finish then orphan)
        for name in removed_names:
            await self.persistence.handle_removed_job(name)

        # 2. Sync New/Changed to Persistence
        for name in (added_names | changed_names):
            await self.persistence.upsert_job(name, new_jobs[name])

        # 3. Re-validate queued/waiting jobs against new graph (handles dependency graph changes)
        await self.persistence.revalidate_jobs(new_jobs)

    def get_job(self, name: str) -> Optional[JobDefinition]:
        return self.jobs.get(name)

    def get_all_jobs(self) -> Dict[str, JobDefinition]:
        return self.jobs.copy()

    def known_job_names(self) -> Set[str]:
        """Names currently backed by a definition, for dispatch eligibility."""
        return set(self.jobs)
