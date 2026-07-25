import aiosqlite
import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from .config import DB_PATH

logger = logging.getLogger(__name__)

class LogStore:
    """Handles storage and retrieval of job run logs."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    async def store_log_chunk(
        self, job_run_id: str, stream: str, chunk: str
    ) -> None:
        """
        Store a stdout/stderr chunk for a job run.

        Args:
            job_run_id: The ID of the job run
            stream: Either 'stdout' or 'stderr'
            chunk: The log chunk to store
        """
        if stream not in ('stdout', 'stderr'):
            raise ValueError(f"Invalid stream '{stream}'. Must be 'stdout' or 'stderr'.")

        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    '''
                    INSERT INTO job_logs (job_run_id, stream, chunk)
                    VALUES (?, ?, ?)
                    ''',
                    (job_run_id, stream, chunk)
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to store log chunk for run {job_run_id}: {e}")
            raise

    async def store_log_chunks(
        self, job_run_id: str, chunks: List[Tuple[str, str]]
    ) -> None:
        """Store a batch of (stream, chunk) pairs in one transaction.

        Log lines used to be written one at a time, each opening its own
        connection and committing, so a job emitting 10000 lines opened
        10000 connections.
        """
        if not chunks:
            return
        for stream, _ in chunks:
            if stream not in ('stdout', 'stderr'):
                raise ValueError(
                    f"Invalid stream '{stream}'. Must be 'stdout' or 'stderr'."
                )
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    "INSERT INTO job_logs (job_run_id, stream, chunk) VALUES (?, ?, ?)",
                    [(job_run_id, stream, chunk) for stream, chunk in chunks],
                )
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to store log chunks for run {job_run_id}: {e}")
            raise

    async def get_logs(self, job_run_id: str) -> List[Tuple[str, str, str]]:
        """
        Retrieve all logs for a job run.

        Args:
            job_run_id: The ID of the job run

        Returns:
            List of (stream, chunk, timestamp) tuples
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    '''
                    SELECT stream, chunk, timestamp
                    FROM job_logs
                    WHERE job_run_id = ?
                    ORDER BY id
                    ''',
                    (job_run_id,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    return [(row['stream'], row['chunk'], row['timestamp']) for row in rows]
        except Exception as e:
            logger.error(f"Failed to retrieve logs for run {job_run_id}: {e}")
            raise

    async def get_logs_for_job(self, job_name: str) -> List[Tuple[str, str, str]]:
        """
        Retrieve all logs for the most recent run of a job.

        Args:
            job_name: The name of the job

        Returns:
            List of (stream, chunk, timestamp) tuples
        """
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                # Get the most recent run_id for this job
                async with db.execute(
                    '''
                    SELECT run_id FROM job_runs
                    WHERE job_name = ?
                    ORDER BY start_time DESC
                    LIMIT 1
                    ''',
                    (job_name,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        return []
                    run_id = row['run_id']

                # Get logs for that run
                async with db.execute(
                    '''
                    SELECT stream, chunk, timestamp
                    FROM job_logs
                    WHERE job_run_id = ?
                    ORDER BY id
                    ''',
                    (run_id,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    return [(row['stream'], row['chunk'], row['timestamp']) for row in rows]
        except Exception as e:
            logger.error(f"Failed to retrieve logs for job {job_name}: {e}")
            raise
