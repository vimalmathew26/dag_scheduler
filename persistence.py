import aiosqlite
import json
import asyncio
import logging
from typing import Optional, Dict, List, Any, Set
from pathlib import Path
from .config import DB_PATH
from .models import JobState, JobDefinition

logger = logging.getLogger(__name__)

class InvalidTransitionError(Exception):
    """Raised when an invalid job state transition is attempted."""
    def __init__(self, job_name: str, from_state: JobState, to_state: JobState):
        self.job_name = job_name
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Invalid transition for job '{job_name}': {from_state} -> {to_state}")


class Persistence:
    """Handles all database operations with SQLite in WAL mode."""
    
    VALID_TRANSITIONS = {
        # Normal forward transitions
        (JobState.DEFINED, JobState.QUEUED),
        (JobState.DEFINED, JobState.WAITING),
        (JobState.WAITING, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.DONE),
        (JobState.RUNNING, JobState.FAILED),
        (JobState.RUNNING, JobState.TIMED_OUT),
        (JobState.RUNNING, JobState.UNKNOWN),
        (JobState.QUEUED, JobState.BLOCKED_UNRESOLVABLE),
        (JobState.BLOCKED_UNRESOLVABLE, JobState.WAITING),
        (JobState.BLOCKED_UNRESOLVABLE, JobState.QUEUED),
        # Cancellation transitions (#7)
        (JobState.QUEUED, JobState.CANCELLED),
        (JobState.RUNNING, JobState.CANCELLED),
        # Re-enqueue transitions for retry (#2, #8)
        (JobState.FAILED, JobState.QUEUED),
        (JobState.FAILED, JobState.WAITING),
        # Reset transitions: terminal states back to DEFINED (#8)
        (JobState.DONE, JobState.DEFINED),
        (JobState.FAILED, JobState.DEFINED),
        (JobState.TIMED_OUT, JobState.DEFINED),
        (JobState.UNKNOWN, JobState.DEFINED),
        (JobState.CANCELLED, JobState.DEFINED),
    }
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
    
    async def setup(self):
        """Initialize database with WAL mode and create tables."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('PRAGMA journal_mode=WAL;')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    name TEXT PRIMARY KEY,
                    definition TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'defined',
                    current_priority INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS job_runs (
                    run_id TEXT PRIMARY KEY,
                    job_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    exit_code INTEGER,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    FOREIGN KEY (job_name) REFERENCES jobs(name)
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_run_id TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    chunk TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (job_run_id) REFERENCES job_runs(run_id)
                )
            ''')
            await db.commit()

    async def upsert_job(self, name: str, definition: JobDefinition):
        """Insert or update a job definition."""
        definition_json = definition.model_dump_json()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO jobs (name, definition, state, current_priority, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(name) DO UPDATE SET
                    definition=excluded.definition,
                    current_priority=excluded.current_priority,
                    updated_at=CURRENT_TIMESTAMP
            ''', (name, definition_json, JobState.DEFINED, definition.priority))
            await db.commit()

    async def handle_removed_job(self, name: str):
        """Handle job removal: transition queued to blocked, let running finish."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM jobs WHERE name = ?", (name,)) as cursor:
                row = await cursor.fetchone()
                if not row: return
                current_state = JobState(row[0])

            if current_state == JobState.QUEUED:
                # Validate and apply the transition within the same connection
                self.validate_transition(name, current_state, JobState.BLOCKED_UNRESOLVABLE)
                await db.execute(
                    "UPDATE jobs SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
                    (JobState.BLOCKED_UNRESOLVABLE.value, name)
                )
                await db.commit()
            elif current_state not in (JobState.RUNNING,):
                await db.execute("DELETE FROM jobs WHERE name = ?", (name,))
                await db.commit()

    async def get_all_db_jobs(self) -> Dict[str, Dict[str, Any]]:
        """Get all jobs and their states from DB."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT name, state, definition FROM jobs") as cursor:
                rows = await cursor.fetchall()
                return {row['name']: {'state': JobState(row['state']), 'definition': json.loads(row['definition'])} for row in rows}

    async def update_job_state(self, name: str, state: JobState):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM jobs WHERE name = ?", (name,)) as cursor:
                row = await cursor.fetchone()
                if not row: return
                current_state = JobState(row[0])
            
            if current_state == state: return
            self.validate_transition(name, current_state, state)
            await db.execute("UPDATE jobs SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?", (state.value, name))
            await db.commit()

    def validate_transition(self, job_name: str, from_state: JobState, to_state: JobState) -> None:
        if from_state == to_state: return
        if (from_state, to_state) not in self.VALID_TRANSITIONS:
            raise InvalidTransitionError(job_name, from_state, to_state)

    async def revalidate_jobs(self, current_snapshot: Dict[str, JobDefinition]):
        """
        Re-evaluate all jobs in DB (waiting, queued, blocked_unresolvable) 
        against the current graph snapshot.
        """
        db_jobs = await self.get_all_db_jobs()
        for name, info in db_jobs.items():
            state = info['state']
            if state in [JobState.WAITING, JobState.QUEUED, JobState.BLOCKED_UNRESOLVABLE]:
                if name not in current_snapshot:
                    # Still in DB but not in snapshot - handled by handle_removed_job usually,
                    # but here we ensure consistency.
                    if state == JobState.QUEUED:
                         await self.update_job_state(name, JobState.BLOCKED_UNRESOLVABLE)
                    continue

                definition = current_snapshot[name]
                # Check if dependencies exist in the new snapshot
                unresolvable = False
                for dep in definition.depends_on:
                    if dep not in current_snapshot:
                        unresolvable = True
                        break
                
                if unresolvable:
                    if state != JobState.BLOCKED_UNRESOLVABLE:
                        await self.update_job_state(name, JobState.BLOCKED_UNRESOLVABLE)
                else:
                    # Not unresolvable anymore, determine if waiting or queued
                    # (Actual scheduling logic will handle transition to QUEUED based on status of deps, 
                    # but here we can move from BLOCKED -> WAITING at least)
                    if state == JobState.BLOCKED_UNRESOLVABLE:
                        await self.update_job_state(name, JobState.WAITING)

    async def get_next_queued_job(self) -> Optional[str]:
        """Fetch the highest priority job that is currently QUEUED."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                '''
                SELECT name FROM jobs
                WHERE state = ?
                ORDER BY current_priority DESC, created_at ASC
                LIMIT 1
                ''', (JobState.QUEUED.value,)
            ) as cursor:
                row = await cursor.fetchone()
                return row['name'] if row else None

    async def age_queued_priorities(self):
        """Increment priority of all queued jobs (priority aging)."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                '''
                UPDATE jobs
                SET current_priority = current_priority + 1
                WHERE state = ?
                ''', (JobState.QUEUED.value,)
            )
            await db.commit()

    async def reset_job_state(self, name: str) -> bool:
        """Reset a job in a terminal state back to DEFINED.

        Returns True if the job was reset, False if the job was not found.
        Raises InvalidTransitionError if the job is not in a terminal state.
        """
        terminal_states = {
            JobState.DONE, JobState.FAILED, JobState.TIMED_OUT,
            JobState.UNKNOWN, JobState.CANCELLED,
        }
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM jobs WHERE name = ?", (name,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return False
                current_state = JobState(row[0])

            if current_state not in terminal_states:
                raise InvalidTransitionError(name, current_state, JobState.DEFINED)

            self.validate_transition(name, current_state, JobState.DEFINED)
            await db.execute(
                "UPDATE jobs SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
                (JobState.DEFINED.value, name)
            )
            await db.commit()
            return True

    async def record_run(self, run):
        """Record a new job run in the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                '''
                INSERT INTO job_runs (run_id, job_name, state, start_time, attempt)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (run.run_id, run.job_name, run.state.value, run.start_time, run.attempt)
            )
            await db.commit()

    async def finalize_run(self, run_id: str, state: 'JobState', exit_code: int):
        """Finalize a job run with end state and exit code."""
        import time
        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                '''
                UPDATE job_runs
                SET state = ?, end_time = ?, exit_code = ?
                WHERE run_id = ?
                ''',
                (state.value, end_time, exit_code, run_id)
            )
            await db.commit()
        return end_time
