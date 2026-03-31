# DAG Scheduler

## Overview

This is a **DAG-based task scheduler daemon** written in Python (≥3.11). It manages job definitions with dependencies, executes them as subprocesses, and supports hot-reloading, retries, priority aging, and a REST API. The entry point is `dag-scheduler`, which maps to `dag_scheduler.__main__:main`.

---

## Architecture

The system is composed of the following core components, wired together in the `Daemon` class:

| Component | File | Responsibility |
|---|---|---|
| **Daemon** | __main__.py | Bootstraps all components, manages lifecycle and signal handling |
| **Persistence** | persistence.py | SQLite (WAL mode) storage for jobs, runs, and logs; enforces state machine transitions |
| **Registry** | registry.py | In-memory job namespace; handles atomic hot-reload swaps and diffing |
| **DefinitionParser** | definition_parser.py | Parses YAML/TOML job files in two passes with cycle detection |
| **DAG** | dag.py | Topological sort (Kahn's algorithm), cycle detection, dependency fan-out |
| **Scheduler** | scheduler.py | Main polling loop; dispatches queued jobs, manages priority aging |
| **Executor** | executor.py | Runs subprocesses with concurrency semaphore, timeout enforcement, log streaming |
| **RetryEngine** | retry_engine.py | Exponential backoff with jitter; decides whether to retry based on exit codes |
| **ProcessManager** | process_manager.py | Tracks live subprocess handles; crash recovery (marks orphaned jobs as `unknown`) |
| **LogStore** | log_store.py | Stores and retrieves stdout/stderr chunks per job run |
| **FileWatcher** | file_watcher.py | Watches jobs directory via `watchdog`; debounces and triggers registry reload |
| **API** | api.py | FastAPI REST endpoints for status, triggering, cancellation, logs, and stats |
| **CLI** | cli.py | Click-based CLI that talks to the API (`status`, `trigger`, `logs`, `runs`, `stats`, `cancel`, `load`) |
| **Config** | config.py | Central constants (paths, concurrency, defaults, ports) |

### Component Dependency Graph

```
Daemon
 ├── Persistence
 ├── LogStore
 ├── ProcessManager ──► Persistence
 ├── Registry ──► Persistence, DefinitionParser ──► DAG
 ├── Executor ──► Persistence, ProcessManager, LogStore, RetryEngine
 ├── Scheduler ──► Persistence, Registry, Executor
 ├── RetryEngine ──► Scheduler
 ├── FileWatcher ──► Registry
 └── API ──► Scheduler, Registry, Persistence, LogStore
```

---

## Data Model

Defined in models.py:

- **`JobState`** — 9-state enum: `defined → waiting → queued → running → done|failed|timed_out|unknown|blocked_unresolvable`
- **`RetryPolicy`** — `max_attempts`, `backoff_base`, `jitter`, `retry_on_exit_codes`
- **`JobDefinition`** — `command`, `depends_on`, `tags`, `priority`, `timeout`, `retry`
- **`JobRun`** — `job_name`, `run_id`, `state`, `start_time`, `end_time`, `exit_code`, `attempt`
- **`DefinitionFile`** — Top-level wrapper with a jobs dict

### State Machine

Valid transitions are enforced by `Persistence.VALID_TRANSITIONS`:

```
defined ──► queued / waiting
waiting ──► queued
queued  ──► running / blocked_unresolvable
running ──► done / failed / timed_out / unknown
blocked_unresolvable ──► waiting / queued
```

An `InvalidTransitionError` is raised for any illegal transition.

### Database Schema (SQLite, WAL mode)

Three tables created in `Persistence.setup()`:

| Table | Purpose |
|---|---|
| jobs | Job definitions, current state, priority |
| `job_runs` | Individual execution records per job |
| `job_logs` | Stdout/stderr chunks per run |

---

## Key Workflows

### 1. Startup (`Daemon.run()`)

1. Initialize DB schema (`persistence.setup()`)
2. Crash recovery — mark orphaned `RUNNING` jobs as `UNKNOWN` (`ProcessManager.handle_crash_recovery()`)
3. Load job definitions from jobs directory (`Registry.load_initial()`)
4. Wire up API dependencies (`init_api()`)
5. Start scheduler polling loop + priority aging loop
6. Start file watcher
7. Start Uvicorn API server

### 2. Job Definition Parsing (`DefinitionParser.parse_directory()`)

Three-pass approach:
1. **Pass 1** — Collect all job names, check for duplicates and required fields
2. **Pass 2** — Validate dependency references exist; construct `JobDefinition` objects
3. **Pass 3** — Cycle detection via `topological_sort()` (Kahn's algorithm)

Supports both YAML and TOML formats. Example files: etl.yaml, maintenance.toml.

### 3. Job Scheduling (`Scheduler._main_loop()`)

- Polls for the highest-priority `QUEUED` job (`_get_next_queued_job()`)
- Dispatches to [`Executor.run_job()`](executor.py) via `asyncio.create_task()`
- Priority aging: every [`PRIORITY_AGING_INTERVAL`](config.py) (60s), all queued jobs get `current_priority += 1` (`_aging_loop()`)

### 4. Job Execution (`Executor.run_job()`)

1. Acquire concurrency semaphore (`MAX_CONCURRENT` = 4)
2. Transition job to `RUNNING`
3. Spawn subprocess via `asyncio.create_subprocess_shell()`
4. Stream stdout/stderr to `LogStore`
5. Enforce timeout via `asyncio.wait_for()`
6. On success (`exit_code == 0`): mark `DONE`, trigger `handle_job_completion()` for dependency fan-out
7. On failure: delegate to `RetryEngine.handle_retry()`
8. On timeout: kill process, mark `TIMED_OUT`

### 5. Retry Logic (`RetryEngine`)

- [`should_retry()`](retry_engine.py): checks `attempt < max_attempts` and `exit_code ∈ retry_on_exit_codes`
- `calculate_backoff()`: $\text{backoff} = \text{backoff\_base}^{\text{attempt}}$, with optional ±20% jitter
- Schedules retry via `asyncio.sleep(backoff)` then re-enqueue

### 6. Hot Reload (`FileWatcher` → `Registry.reload()`)

1. `watchdog` detects file changes in jobs
2. Debounced (500ms) to coalesce rapid edits
3. [`Registry.reload()`](registry.py) parses all files atomically under `_reload_lock`
4. `_handle_diff()` computes added/removed/changed jobs
5. Removed jobs transition `queued → blocked_unresolvable`
6. `Persistence.revalidate_jobs()` re-checks dependency validity

### 7. Dependency Fan-Out (`Scheduler.handle_job_completion()`)

When a job completes as `DONE`:
- [`get_ready_jobs()`](dag.py) finds direct dependents whose **all** dependencies are now `done`
- Those jobs transition `waiting → queued`

---

## API Endpoints (api.py)

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Root message |
| `GET` | `/health` | Alive check |
| `GET` | jobs | List all jobs (filter by `state`, `tag`) |
| `GET` | `/jobs/{job_id}` | Job detail + last run summary |
| `GET` | `/jobs/{job_id}/runs` | Run history |
| `GET` | `/jobs/{job_id}/runs/{run_id}/logs` | Stdout/stderr for a run |
| `POST` | `/jobs/{job_id}/trigger` | Force-queue (bypass dependencies) |
| `POST` | `/jobs/{job_id}/cancel` | Cancel queued/running job |
| `GET` | `/stats` | Aggregate stats (pass rate, avg duration, jobs by state) |

---

## CLI Commands (cli.py)

| Command | Description |
|---|---|
| `dag-scheduler load <path>` | Copy definition file to jobs for hot-reload |
| `dag-scheduler status` | Show all jobs and states |
| `dag-scheduler trigger <name>` | Force-queue a job |
| `dag-scheduler logs <name>` | Tail logs for most recent run |
| `dag-scheduler runs <name>` | Show run history |
| `dag-scheduler stats` | Show aggregate statistics |
| `dag-scheduler cancel <name>` | Cancel a job |

---

## Job Definition Examples

**ETL Pipeline** (etl.yaml): `extract_data → transform_data → load_data` — a 3-stage chain with retries and priorities.

**Maintenance** (maintenance.toml): `cleanup_logs` (depends on `load_data`) → `archive_old` — cross-file dependencies resolved via flat namespace.

**Failing Job** (failing.yaml): `always_fails` — exits with code 1, retries 3 times with no jitter.

**Timeout Test** (slow.yaml): `slow_job` — `sleep 30` with a 2s timeout, demonstrating timeout handling.

**Cycle Detection** (cycle.yaml): `job_a ↔ job_b` — mutual dependency that triggers `CycleError` during parsing.
