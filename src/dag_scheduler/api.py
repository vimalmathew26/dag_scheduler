# api.py - FastAPI endpoints for the DAG scheduler

import json
import logging
import secrets
from typing import Any

import aiosqlite
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from . import metrics
from .config import API_TOKEN
from .models import JobState
from .persistence import InvalidTransitionError

logger = logging.getLogger(__name__)

app = FastAPI(title="DAG Scheduler API", version="0.2.0")

# Reads stay open on loopback. The mutating routes are gated, because any
# local process, including a web page issuing a cross-origin POST, could
# otherwise trigger or cancel jobs.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def require_token(authorization: str | None = Header(default=None)) -> None:
    """Gate a mutating endpoint behind a shared token.

    If DAG_SCHEDULER_TOKEN is unset the daemon runs unauthenticated, which
    is reasonable for a single-node daemon bound to loopback and is what the
    startup log warns about. Deliberately not users and roles: this is one
    process on one machine, and the real privilege boundary is write access
    to the jobs directory, since definitions run as shell commands.
    """
    if not API_TOKEN:
        return
    expected = f"Bearer {API_TOKEN}"
    if not authorization or not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Missing or invalid token")


# --------------- Dependency injection helpers ---------------


def init_api(sched: Any, reg: Any, pers: Any, logs: Any, proc_mgr: Any) -> None:
    """Initialize API with dependencies stored on app.state."""
    app.state.scheduler = sched
    app.state.registry = reg
    app.state.persistence = pers
    app.state.log_store = logs
    app.state.process_manager = proc_mgr
    app.state.db_path = pers.db_path


def _get_scheduler(request: Request) -> Any:
    return request.app.state.scheduler


def _get_registry(request: Request) -> Any:
    return request.app.state.registry


def _get_persistence(request: Request) -> Any:
    return request.app.state.persistence


def _get_log_store(request: Request) -> Any:
    return request.app.state.log_store


def _get_process_manager(request: Request) -> Any:
    return request.app.state.process_manager


def _get_db_path(request: Request) -> Any:
    """The database this daemon instance was started against.

    Routes used to open the module-global config.DB_PATH directly, which
    made the API impossible to point at a test database.
    """
    return request.app.state.db_path


@app.get("/")
async def root() -> dict[str, str]:
    return {"message": "DAG Scheduler API"}


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Daemon alive check"""
    return {"status": "ok"}


@app.get("/jobs")
async def list_jobs(
    state: JobState | None = Query(None, description="Filter by job state"),
    tag: str | None = Query(None, description="Filter by tag"),
    db_path: Any = Depends(_get_db_path),
) -> list[dict[str, Any]]:
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
                definition = json.loads(row["definition"])
                # Filter by tag if specified
                if tag and tag not in definition.get("tags", []):
                    continue

                jobs.append(
                    {
                        "name": row["name"],
                        "state": row["state"],
                        "tags": definition.get("tags", []),
                        "priority": definition.get("priority", 1),
                    }
                )

            return jobs
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.get("/jobs/{job_id}")
async def get_job_detail(
    job_id: str,
    db_path: Any = Depends(_get_db_path),
) -> dict[str, Any]:
    """Get single job detail + last run summary"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Get job details
            async with db.execute(
                "SELECT name, state, definition FROM jobs WHERE name = ?", (job_id,)
            ) as cursor:
                job_row = await cursor.fetchone()

            if not job_row:
                raise HTTPException(status_code=404, detail="Job not found")

            definition = json.loads(job_row["definition"])

            # Get last run
            async with db.execute(
                """
                SELECT run_id, state, start_time, end_time, exit_code, attempt
                FROM job_runs
                WHERE job_name = ?
                ORDER BY start_time DESC
                LIMIT 1
                """,
                (job_id,),
            ) as cursor:
                run_row = await cursor.fetchone()

            last_run = dict(run_row) if run_row else None

            return {
                "name": job_row["name"],
                "state": job_row["state"],
                "definition": definition,
                "last_run": last_run,
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.get("/jobs/{job_id}/runs")
async def get_job_runs(
    job_id: str,
    db_path: Any = Depends(_get_db_path),
) -> list[dict[str, Any]]:
    """Get run history for a job"""
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row

            # Verify job exists
            async with db.execute("SELECT name FROM jobs WHERE name = ?", (job_id,)) as cursor:
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
                (job_id,),
            ) as cursor:
                rows = await cursor.fetchall()

            return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job runs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.get("/jobs/{job_id}/runs/{run_id}/logs")
async def get_run_logs(
    job_id: str,
    run_id: str,
    logs: Any = Depends(_get_log_store),
    db_path: Any = Depends(_get_db_path),
) -> list[dict[str, Any]]:
    """Get stdout/stderr for a run"""
    try:
        # Verify run exists and belongs to job
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT run_id FROM job_runs WHERE run_id = ? AND job_name = ?", (run_id, job_id)
            ) as cursor:
                if not await cursor.fetchone():
                    raise HTTPException(status_code=404, detail="Run not found")

        # Get logs
        log_entries = await logs.get_logs(run_id)
        return [
            {"stream": stream, "chunk": chunk, "timestamp": timestamp}
            for stream, chunk, timestamp in log_entries
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting run logs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.post("/jobs/{job_id}/trigger", dependencies=[Depends(require_token)])
async def trigger_job(
    job_id: str,
    reg: Any = Depends(_get_registry),
    sched: Any = Depends(_get_scheduler),
) -> dict[str, str]:
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
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.post("/jobs/{job_id}/cancel", dependencies=[Depends(require_token)])
async def cancel_job(
    job_id: str,
    pers: Any = Depends(_get_persistence),
    proc_mgr: Any = Depends(_get_process_manager),
    db_path: Any = Depends(_get_db_path),
) -> dict[str, str]:
    """Cancel a queued or running job"""
    try:
        # Get current job state
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT state FROM jobs WHERE name = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Job not found")

            current_state = JobState(row["state"])

        # Only cancel QUEUED or RUNNING jobs
        if current_state not in [JobState.QUEUED, JobState.RUNNING]:
            raise HTTPException(
                status_code=400, detail=f"Cannot cancel job in state {current_state}"
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
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.get("/metrics", response_class=PlainTextResponse)
async def get_metrics(db_path: Any = Depends(_get_db_path)) -> str:
    """Prometheus text exposition of process counters and queue gauges."""
    async with (
        aiosqlite.connect(db_path) as db,
        db.execute(
            "SELECT state, COUNT(*) FROM jobs WHERE state IN (?, ?) GROUP BY state",
            (JobState.RUNNING.value, JobState.QUEUED.value),
        ) as cursor,
    ):
        counts = {row[0]: row[1] for row in await cursor.fetchall()}

    metrics.set_gauge("dag_running_jobs", counts.get(JobState.RUNNING.value, 0))
    metrics.set_gauge("dag_queued_jobs", counts.get(JobState.QUEUED.value, 0))
    return metrics.render()


@app.get("/stats")
async def get_statistics(db_path: Any = Depends(_get_db_path)) -> dict[str, Any]:
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
                total_runs = (stats_row["total_runs"] if stats_row else 0) or 0
                passed_runs = (stats_row["passed_runs"] if stats_row else 0) or 0
                pass_rate = passed_runs / total_runs if total_runs > 0 else 0

            # Get average duration
            async with db.execute("""
                SELECT AVG(strftime('%s', end_time) - strftime('%s', start_time)) as avg_duration
                FROM job_runs
                WHERE end_time IS NOT NULL
            """) as cursor:
                duration_row = await cursor.fetchone()
                avg_duration = (duration_row["avg_duration"] if duration_row else 0) or 0

            # Get jobs by state
            async with db.execute("""
                SELECT state, COUNT(*) as count
                FROM jobs
                GROUP BY state
            """) as cursor:
                rows = await cursor.fetchall()
                jobs_by_state = {row["state"]: row["count"] for row in rows}

            return {
                "total_runs": total_runs,
                "pass_rate": pass_rate,
                "avg_duration_seconds": avg_duration,
                "jobs_by_state": jobs_by_state,
            }
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e


@app.post("/jobs/{job_id}/reset", dependencies=[Depends(require_token)])
async def reset_job(
    job_id: str,
    pers: Any = Depends(_get_persistence),
) -> dict[str, str]:
    """Reset a job in a terminal state (done/failed/timed_out/unknown/cancelled) back to defined."""
    try:
        result = await pers.reset_job_state(job_id)
        if not result:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"status": "success", "message": f"Job {job_id} reset to defined"}
    except InvalidTransitionError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resetting job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e
