# api.py - FastAPI endpoints for the DAG scheduler

import logging
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query, Depends, Request
import aiosqlite
import json

from .models import JobState

logger = logging.getLogger(__name__)

app = FastAPI(title="DAG Scheduler API", version="0.1.0")


# --------------- Dependency injection helpers ---------------

def init_api(sched, reg, pers, logs, proc_mgr):
    """Initialize API with dependencies stored on app.state."""
    app.state.scheduler = sched
    app.state.registry = reg
    app.state.persistence = pers
    app.state.log_store = logs
    app.state.process_manager = proc_mgr
    app.state.db_path = pers.db_path


def _get_scheduler(request: Request):
    return request.app.state.scheduler

def _get_registry(request: Request):
    return request.app.state.registry

def _get_persistence(request: Request):
    return request.app.state.persistence

def _get_log_store(request: Request):
    return request.app.state.log_store

def _get_process_manager(request: Request):
    return request.app.state.process_manager

def _get_db_path(request: Request):
    """The database this daemon instance was started against.

    Routes used to open the module-global config.DB_PATH directly, which
    made the API impossible to point at a test database.
    """
    return request.app.state.db_path


@app.get("/")
async def root():
    return {"message": "DAG Scheduler API"}


@app.get("/health")
async def health_check():
    """Daemon alive check"""
    return {"status": "ok"}


@app.get("/jobs")
async def list_jobs(
    state: Optional[JobState] = Query(None, description="Filter by job state"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    db_path=Depends(_get_db_path),
) -> List[Dict[str, Any]]:
    """List all jobs with current state, filter by state and tag"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Build query with optional filters
            query = "SELECT name, state, definition FROM jobs"
            params = []

            if state:
                query += " WHERE state = ?"
                params.append(state.value)

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()

            jobs = []
            for row in rows:
                definition = json.loads(row['definition'])
                # Filter by tag if specified
                if tag and tag not in definition.get('tags', []):
                    continue

                jobs.append({
                    "name": row['name'],
                    "state": row['state'],
                    "tags": definition.get('tags', []),
                    "priority": definition.get('priority', 1)
                })

            return jobs
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/jobs/{job_id}")
async def get_job_detail(
    job_id: str,
    db_path=Depends(_get_db_path),
) -> Dict[str, Any]:
    """Get single job detail + last run summary"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Get job details
            async with db.execute(
                "SELECT name, state, definition FROM jobs WHERE name = ?",
                (job_id,)
            ) as cursor:
                job_row = await cursor.fetchone()

            if not job_row:
                raise HTTPException(status_code=404, detail="Job not found")

            definition = json.loads(job_row['definition'])

            # Get last run
            async with db.execute(
                """
                SELECT run_id, state, start_time, end_time, exit_code, attempt
                FROM job_runs
                WHERE job_name = ?
                ORDER BY start_time DESC
                LIMIT 1
                """,
                (job_id,)
            ) as cursor:
                run_row = await cursor.fetchone()

            last_run = dict(run_row) if run_row else None

            return {
                "name": job_row['name'],
                "state": job_row['state'],
                "definition": definition,
                "last_run": last_run
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/jobs/{job_id}/runs")
async def get_job_runs(
    job_id: str,
    db_path=Depends(_get_db_path),
) -> List[Dict[str, Any]]:
    """Get run history for a job"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Verify job exists
            async with db.execute(
                "SELECT name FROM jobs WHERE name = ?",
                (job_id,)
            ) as cursor:
                if not await cursor.fetchone():
                    raise HTTPException(status_code=404, detail="Job not found")

            # Get runs
            async with db.execute(
                """
                SELECT run_id, state, start_time, end_time, exit_code, attempt
                FROM job_runs
                WHERE job_name = ?
                ORDER BY start_time DESC
                """,
                (job_id,)
            ) as cursor:
                rows = await cursor.fetchall()

            return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job runs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/jobs/{job_id}/runs/{run_id}/logs")
async def get_run_logs(
    job_id: str,
    run_id: str,
    logs=Depends(_get_log_store),
    db_path=Depends(_get_db_path),
) -> List[Dict[str, Any]]:
    """Get stdout/stderr for a run"""
    try:
        # Verify run exists and belongs to job
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT run_id FROM job_runs WHERE run_id = ? AND job_name = ?",
                (run_id, job_id)
            ) as cursor:
                if not await cursor.fetchone():
                    raise HTTPException(status_code=404, detail="Run not found")

        # Get logs
        log_entries = await logs.get_logs(run_id)
        return [{"stream": stream, "chunk": chunk, "timestamp": timestamp}
                for stream, chunk, timestamp in log_entries]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run logs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/jobs/{job_id}/trigger")
async def trigger_job(
    job_id: str,
    reg=Depends(_get_registry),
    sched=Depends(_get_scheduler),
) -> Dict[str, str]:
    """Force-queue a job bypassing dependency check (manual trigger)"""
    try:
        # Check if job exists
        job_def = reg.get_job(job_id)
        if not job_def:
            raise HTTPException(status_code=404, detail="Job not found")

        # Enqueue job bypassing dependencies
        await sched.enqueue_job(job_id, bypass_deps=True)

        return {"status": "success", "message": f"Job {job_id} triggered"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    pers=Depends(_get_persistence),
    proc_mgr=Depends(_get_process_manager),
    db_path=Depends(_get_db_path),
) -> Dict[str, str]:
    """Cancel a queued or running job"""
    try:
        # Get current job state
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT state FROM jobs WHERE name = ?",
                (job_id,)
            ) as cursor:
                row = await cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Job not found")

            current_state = JobState(row['state'])

        # Only cancel QUEUED or RUNNING jobs
        if current_state not in [JobState.QUEUED, JobState.RUNNING]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel job in state {current_state}"
            )

        # Mark the job cancelled before killing anything, so the executor
        # sees the cancellation and records the run as cancelled rather than
        # writing its own outcome over it.
        await pers.update_job_state(job_id, JobState.CANCELLED)

        started = False
        if current_state == JobState.RUNNING:
            started = await proc_mgr.kill_by_job_name(job_id)
            if started:
                logger.info(f"Process for job '{job_id}' killed on cancel")

        if not started:
            # Nothing was ever spawned: either the job was still queued, or
            # it was claimed but had not reached a concurrency slot. The run
            # record carries a NULL exit code because no process exited.
            await pers.record_cancelled_run(job_id)

        return {"status": "success", "message": f"Job {job_id} cancelled"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stats")
async def get_statistics(db_path=Depends(_get_db_path)) -> Dict[str, Any]:
    """Get aggregate statistics"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Get total runs and pass rate
            async with db.execute("""
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN state = 'done' THEN 1 ELSE 0 END) as passed_runs
                FROM job_runs
            """) as cursor:
                stats_row = await cursor.fetchone()
                total_runs = stats_row['total_runs'] or 0
                passed_runs = stats_row['passed_runs'] or 0
                pass_rate = passed_runs / total_runs if total_runs > 0 else 0

            # Get average duration
            async with db.execute("""
                SELECT AVG(strftime('%s', end_time) - strftime('%s', start_time)) as avg_duration
                FROM job_runs
                WHERE end_time IS NOT NULL
            """) as cursor:
                duration_row = await cursor.fetchone()
                avg_duration = duration_row['avg_duration'] or 0

            # Get jobs by state
            async with db.execute("""
                SELECT state, COUNT(*) as count
                FROM jobs
                GROUP BY state
            """) as cursor:
                rows = await cursor.fetchall()
                jobs_by_state = {row['state']: row['count'] for row in rows}

            return {
                "total_runs": total_runs,
                "pass_rate": pass_rate,
                "avg_duration_seconds": avg_duration,
                "jobs_by_state": jobs_by_state
            }
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/jobs/{job_id}/reset")
async def reset_job(
    job_id: str,
    pers=Depends(_get_persistence),
) -> Dict[str, str]:
    """Reset a job in a terminal state (done/failed/timed_out/unknown/cancelled) back to defined."""
    try:
        from .persistence import InvalidTransitionError
        result = await pers.reset_job_state(job_id)
        if not result:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"status": "success", "message": f"Job {job_id} reset to defined"}
    except InvalidTransitionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
