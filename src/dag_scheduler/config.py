"""Configuration for the DAG scheduler.

Importing this module has no side effects. Paths are resolved here but
nothing is created: directory creation happens once, explicitly, at daemon
startup via ensure_directories().

Anything that changes per deployment reads from the environment. Anything
that is genuinely a property of the design stays a constant.

Per-job defaults deliberately do not live here. They belong to the schema
and are declared once on the pydantic models in models.py; keeping a second
copy meant the same five numbers were written down in three places.
"""

import os
from pathlib import Path

# --------------------------------------------------------------------------
# Environment-overridable: these change per machine and per deployment.
# --------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# Paths
BASE_DIR = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share")) / "dag_scheduler"

DB_PATH = Path(os.environ.get("DAG_SCHEDULER_DB", BASE_DIR / "scheduler.db"))

# Job definitions belong to the operator, not to the installed package. This
# used to resolve inside the package directory, so after a pip install
# `dag-scheduler load` wrote user files into site-packages.
JOBS_DIR = Path(os.environ.get("DAG_SCHEDULER_JOBS_DIR", Path.cwd() / "jobs"))

# Execution
MAX_CONCURRENT = _env_int("DAG_SCHEDULER_MAX_CONCURRENT", 4)

# API
API_HOST = os.environ.get("DAG_SCHEDULER_HOST", "127.0.0.1")
API_PORT = _env_int("DAG_SCHEDULER_PORT", 8000)
API_TOKEN: str | None = os.environ.get("DAG_SCHEDULER_TOKEN") or None

# Scheduler
PRIORITY_AGING_INTERVAL = _env_float("DAG_SCHEDULER_AGING_INTERVAL", 60.0)

# Logging
LOG_LEVEL = os.environ.get("DAG_SCHEDULER_LOG_LEVEL", "INFO")
LOG_JSON = _env_bool("DAG_SCHEDULER_LOG_JSON")

# --------------------------------------------------------------------------
# Constants: properties of the design, not of a deployment. Nobody needs to
# tune these, and exposing them would only invite drift.
# --------------------------------------------------------------------------

GRACEFUL_KILL_TIMEOUT = 5  # seconds between SIGTERM and SIGKILL
SHUTDOWN_TIMEOUT = 10  # seconds to drain the API and in-flight jobs
LOG_BATCH_SIZE = 100  # log lines buffered before a write
LOG_FLUSH_INTERVAL = 0.5  # seconds before a partial log buffer is written


def ensure_directories(
    db_path: Path | None = None,
    jobs_dir: Path | None = None,
) -> None:
    """Create the directories the daemon needs.

    Called once from the daemon entry point, never at import time.
    """
    (db_path or DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    (jobs_dir or JOBS_DIR).mkdir(parents=True, exist_ok=True)
