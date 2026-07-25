import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .persistence import Persistence
from .config import JOBS_DIR
from .definition_parser import DefinitionParser
from .models import JobDefinition

logger = logging.getLogger(__name__)


class Registry:
    """
    Manages the flat job namespace, handling atomic hot-reload swaps,
    diffing definitions, and transitioning removed jobs.
    """

    def __init__(self, persistence: "Persistence", jobs_dir: Path | None = None) -> None:
        self.persistence = persistence
        self.jobs_dir = Path(jobs_dir) if jobs_dir is not None else JOBS_DIR
        self.jobs: dict[str, JobDefinition] = {}
        self._reload_lock = asyncio.Lock()

    async def load_initial(self) -> None:
        """Initial load of jobs on startup."""
        async with self._reload_lock:
            parser = DefinitionParser()
            try:
                new_jobs = parser.parse_directory(self.jobs_dir)
                self.jobs = new_jobs
                for name, definition in new_jobs.items():
                    await self.persistence.upsert_job(name, definition)
            except Exception as e:
                # parse_directory reports per-file and per-job problems as
                # warnings and returns whatever loaded cleanly, so reaching
                # here means something unexpected went wrong rather than a
                # bad definition. The daemon logs it and starts anyway.
                logger.error(f"Initial load failed: {e}")
                raise

    async def reload(self) -> None:
        """Reparse the jobs directory and reconcile the database.

        Parsing happens outside the lock and off the event loop, because it
        is blocking file I/O that used to stall the API and the scheduler
        for its duration while holding the reload lock.

        The snapshot swap and the database reconciliation then happen
        together under the lock. They used to be separated: self.jobs was
        replaced first and _handle_diff ran afterwards across a series of
        awaits, so for that window the registry said one thing and the
        database said another, and dispatch reads take no lock at all.
        """
        parser = DefinitionParser()
        try:
            new_jobs = await asyncio.to_thread(parser.parse_directory, self.jobs_dir)
        except Exception as e:
            # The previous snapshot is kept if parsing blows up entirely.
            # Note this is not a validation gate: parse_directory drops
            # bad jobs and returns the rest rather than raising, so a
            # partial result is a normal outcome, not a failure.
            logger.error(f"Reload failed, keeping the previous snapshot: {e}")
            raise

        async with self._reload_lock:
            old_jobs = self.jobs
            await self._handle_diff(old_jobs, new_jobs)
            self.jobs = new_jobs

    async def _handle_diff(
        self,
        old_jobs: dict[str, JobDefinition],
        new_jobs: dict[str, JobDefinition],
    ) -> None:
        """
        Diffs old vs new snapshot and handles removed/changed jobs.
        """
        removed_names = set(old_jobs.keys()) - set(new_jobs.keys())
        added_names = set(new_jobs.keys()) - set(old_jobs.keys())
        changed_names = {
            name
            for name in set(old_jobs.keys()) & set(new_jobs.keys())
            if old_jobs[name] != new_jobs[name]
        }

        # 1. Handle Removed Jobs (transition queued→blocked_unresolvable, running→let finish then orphan)
        for name in removed_names:
            await self.persistence.handle_removed_job(name)

        # 2. Sync New/Changed to Persistence
        for name in added_names | changed_names:
            await self.persistence.upsert_job(name, new_jobs[name])

        # 3. Re-validate queued/waiting jobs against new graph (handles dependency graph changes)
        await self.persistence.revalidate_jobs(new_jobs)

    async def snapshot(self) -> dict[str, JobDefinition]:
        """A consistent view of the namespace, taken under the reload lock.

        The dispatch loop uses this so it cannot observe a half-applied
        reload, where the in-memory namespace has changed but the database
        rows behind it have not been reconciled yet.
        """
        async with self._reload_lock:
            return dict(self.jobs)

    def get_job(self, name: str) -> JobDefinition | None:
        return self.jobs.get(name)

    def get_all_jobs(self) -> dict[str, JobDefinition]:
        return self.jobs.copy()

    def known_job_names(self) -> set[str]:
        """Names currently backed by a definition, for dispatch eligibility."""
        return set(self.jobs)
