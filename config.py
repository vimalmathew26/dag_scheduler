# Configuration constants for the DAG scheduler

import os
from pathlib import Path

# Paths
BASE_DIR = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share')) / 'genie_dag'
BASE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = BASE_DIR / 'scheduler.db'
JOBS_DIR = Path(__file__).parent / 'jobs'
JOBS_DIR.mkdir(exist_ok=True)

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
