"""Configuration constants for the DAG scheduler.

Importing this module has no side effects.  Paths are resolved here but
nothing is created: directory creation happens once, explicitly, at
daemon startup via ensure_directories().

Importing a module used to create directories under the user's data
directory, which meant no test could run without touching real state.
"""

import os
from pathlib import Path
from typing import Optional

# Paths
BASE_DIR = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share')) / 'dag_scheduler'

DB_PATH = BASE_DIR / 'scheduler.db'
# Job definitions belong to the operator, not to the installed package.
# This used to resolve inside the package directory, so after a pip install
# `dag-scheduler load` wrote user files into site-packages and pip uninstall
# deleted them.
JOBS_DIR = Path(os.environ.get('DAG_SCHEDULER_JOBS_DIR', Path.cwd() / 'jobs'))

# Execution settings
MAX_CONCURRENT = 4
DEFAULT_TIMEOUT = 60  # seconds
DEFAULT_RETRY = 3

# API settings
API_PORT = 8000
API_HOST = '127.0.0.1'

# Retry policy defaults
DEFAULT_BACKOFF_BASE = 2.0
DEFAULT_JITTER = True
DEFAULT_RETRY_ON_EXIT_CODES = [1]

# Scheduler settings
PRIORITY_AGING_INTERVAL = 60  # seconds
GRACEFUL_KILL_TIMEOUT = 5  # seconds before SIGKILL after SIGTERM
SHUTDOWN_TIMEOUT = 10  # seconds to wait for the API and in-flight jobs to drain


def ensure_directories(
    db_path: Optional[Path] = None,
    jobs_dir: Optional[Path] = None,
) -> None:
    """Create the directories the daemon needs.

    Called once from the daemon entry point, never at import time.
    """
    (db_path or DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    (jobs_dir or JOBS_DIR).mkdir(parents=True, exist_ok=True)
