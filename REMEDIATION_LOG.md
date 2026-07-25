# Remediation log

Working artifact. Deleted in the final commit.

Before/after observed behaviour for each functional defect. Every capture
was produced by running the daemon, not by reading code or running tests.

## Harness

All reproductions source this. `start_daemon` wipes the data directory so
every run begins with no prior database.

```bash
unset ALL_PROXY all_proxy HTTP_PROXY http_proxy HTTPS_PROXY https_proxy
export NO_PROXY=127.0.0.1,localhost
export XDG_DATA_HOME=/tmp/xdg
B=/tmp/work/dag_scheduler/.venv/bin
REPO=/tmp/work/dag_scheduler
DB=/tmp/xdg/genie_dag/scheduler.db
api(){ ... httpx client against http://127.0.0.1:8000 ... }
dbq(){ ... sqlite3 query against $DB ... }
start_daemon(){ rm -rf /tmp/xdg; mkdir -p /tmp/xdg; cd /tmp/work
  nohup $B/python -m dag_scheduler > "$1" 2>&1 & DAEMON_PID=$!; sleep 6; }
```

Environment note: CPython 3.10 with a `tomllib`/`tomli` shim, because the
sandbox has no 3.11+ and cannot download one. `tomli` is the upstream of
`tomllib`. No finding below depends on this.

All "before" captures were taken at commit 9d7c7a7, after Phase A. Phase A
changed no behaviour: it made two never-awaiting functions synchronous and
moved path resolution out of import time.

---

## B1: one trigger executes a job many times

`scheduler.py:43-48` selects the highest-priority QUEUED row and dispatches
it without claiming it, then loops with no sleep on that branch. The row
stays QUEUED until a dispatched task reaches `executor.py:47`, so the loop
re-reads and re-dispatches the same row repeatedly.

### Command

```bash
bash /tmp/repro/b1.sh
```

```bash
start_daemon /tmp/repro/b1.log
api post /jobs/extract_data/trigger
sleep 12
dbq "select job_name, count(*) from job_runs group by job_name order by job_name"
grep -c "Starting job" /tmp/repro/b1.log
grep -c "Task exception was never retrieved" /tmp/repro/b1.log
wc -l < /tmp/repro/b1.log
```

### BEFORE

```
### one trigger of extract_data, then wait 12s
200 {"status": "success", "message": "Job extract_data triggered"}
### job_runs rows per job (expect exactly 1 each)
('archive_old', 4)
('cleanup_logs', 4)
('extract_data', 6)
('load_data', 4)
('transform_data', 4)
### total subprocess executions logged
22
### unretrieved task exceptions
24
### total daemon log lines
293
```

One trigger of a five-job chain produced 22 subprocess executions and 24
crashed dispatch tasks.

### AFTER

```
### one trigger of extract_data, then wait 12s
200 {"status": "success", "message": "Job extract_data triggered"}
### job_runs rows per job (expect exactly 1 each)
('archive_old', 1)
('cleanup_logs', 1)
('extract_data', 1)
('load_data', 1)
('transform_data', 1)
### total subprocess executions logged
5
### unretrieved task exceptions
0
### total daemon log lines
27
```

Exactly one run per job. 22 executions down to 5, which is the whole
chain. 24 crashed dispatch tasks down to 0. 293 log lines down to 27.

### Fix

Selection and claiming are now one statement. `claim_next_queued_job`
issues a single `UPDATE ... WHERE name = (SELECT ... LIMIT 1) AND
state='queued' RETURNING name`, so a job is handed out exactly once
however fast the loop spins. `update_job_state` became a compare-and-swap
against the state it read, so a transition validated against a stale read
is rejected rather than applied. The loop also refuses to claim while the
executor is at capacity, which bounds the pending task list.

---

## B2: retries never terminate

`retry_engine.py:96` passes `attempt` to `Scheduler.enqueue_job`, whose
signature accepts it (`scheduler.py:64`) and never reads it. The job returns
to QUEUED with no memory of the attempt number and `scheduler.py:48`
dispatches with the default `attempt=1`, so `should_retry` always compares
`1 >= max_attempts`.

### Command

```bash
bash /tmp/repro/b2.sh
```

```bash
start_daemon /tmp/repro/b2.log
api post /jobs/always_fails/trigger      # max_attempts=3, backoff_base=1.0, jitter=false
sleep 20
dbq "select count(*) from job_runs where job_name='always_fails'"
dbq "select attempt, count(*) from job_runs where job_name='always_fails' group by attempt"
dbq "select state from jobs where name='always_fails'"
grep -c "Max retries" /tmp/repro/b2.log
```

### BEFORE

```
### run count
(47,)
### distinct attempt numbers recorded
(1, 47)
### final job state
('queued',)
### did it ever conclude?
0
```

47 executions in 20 seconds for a job configured for 3 attempts, every run
recording `attempt=1`, still queued for another attempt when the daemon was
stopped, and the "max retries exceeded" branch never once reached.

---

## B3: timeout marks the job but never kills the process

`asyncio.wait_for` cancels `_execute_shell`, whose `finally` deregisters the
process at `executor.py:116` before `TimeoutError` propagates. By the time
`_handle_timeout` looks the process up at `executor.py:132` it is gone, so
the SIGTERM/SIGKILL block at `executor.py:136-142` is unreachable.

### Command

```bash
bash /tmp/repro/b3.sh
```

```bash
start_daemon /tmp/repro/b3.log
api post /jobs/slow_job/trigger          # "sleep 30" with timeout 2
sleep 10
dbq "select state from jobs where name='slow_job'"
pgrep -c -x sleep
kill -TERM $DAEMON_PID; sleep 3
pgrep -c -x sleep
```

### BEFORE

```
### job state (expect timed_out)
('timed_out',)
### orphaned 'sleep 30' processes alive 8s after a 2s timeout
4
### detail
     67      10 /bin/sh -c sleep 30
     69      10 sleep 30
     72      10 /bin/sh -c sleep 30
     74      10 sleep 30
### still alive after the daemon exits
4
```

The job is correctly marked `timed_out` while its process keeps running,
and outlives the daemon.

---

## B4: cancelling a running job raises uncaught exceptions

`api.py:261` sets CANCELLED while the executor is still running. The
executor then attempts `cancelled -> timed_out` at `executor.py:149`, which
is outside `run_job`'s try, and `cancelled -> running` from the duplicate
dispatches. Run rows never represent cancellation at all.

### Command

```bash
bash /tmp/repro/b4.sh
```

```bash
start_daemon /tmp/repro/b4.log
api post /jobs/slow_job/trigger
sleep 1
api post /jobs/slow_job/cancel
sleep 4
dbq "select state from jobs where name='slow_job'"
dbq "select state, exit_code, end_time is null as no_end_time from job_runs where job_name='slow_job'"
grep -c "Task exception was never retrieved" /tmp/repro/b4.log
grep -o "Invalid transition for job 'slow_job': [a-z_ ->]*" /tmp/repro/b4.log | sort | uniq -c
```

### BEFORE

```
### job-level state
('cancelled',)
### run-level states (a cancelled run must not say timed_out)
('timed_out', -1, 0)
('timed_out', -1, 0)
('timed_out', -1, 0)
('timed_out', -1, 0)
### unretrieved task exceptions
33
### illegal transitions attempted
     29 Invalid transition for job 'slow_job': cancelled -> running
      4 Invalid transition for job 'slow_job': cancelled -> timed_out
```

Job level is right. Run level claims four timeouts with a sentinel exit
code of -1, and 33 dispatch tasks died.

---

## B5: crash recovery reconciles jobs but not runs

`handle_crash_recovery` (`process_manager.py:38-60`) only reads and writes
the `jobs` table.

### Command

```bash
bash /tmp/repro/b5.sh
```

```bash
start_daemon /tmp/repro/b5a.log
api post /jobs/slow_job/trigger
sleep 2
kill -9 $DAEMON_PID; sleep 2
nohup $B/python -m dag_scheduler > /tmp/repro/b5b.log 2>&1 & DP2=$!
sleep 6
dbq "select state from jobs where name='slow_job'"
dbq "select state, count(*) from job_runs group by state"
dbq "select count(*) from job_runs where state='running' and end_time is null"
```

### BEFORE

```
### before SIGKILL
('running',)
('running', 4)
### job-level state after recovery (expect unknown)
('unknown',)
### run-level rows after recovery (expect none left in 'running')
('running', 4)
### dangling rows with no end_time
(4,)
```

Job level behaves exactly as documented. Four run rows are left claiming to
be executing, permanently, with no end time.

---

## B11: one duplicate name deletes unrelated pipelines

`definition_parser.py:64-70` breaks out of the loop on a duplicate and
discards the whole file. Which file survives depends on `directory.glob()`
enumeration order at `definition_parser.py:45`.

### Command

```bash
bash /tmp/repro/b11.sh
```

```bash
start_daemon /tmp/repro/b11.log
dbq "select count(*) from jobs"; dbq "select name from jobs order by name"
cat > $REPO/jobs/dup.yaml <<'Y'
jobs:
  extract_data:
    command: "echo duplicate"
Y
sleep 4
dbq "select count(*) from jobs"; dbq "select name from jobs order by name"
grep -i "duplicate" /tmp/repro/b11.log | tail -3
```

### BEFORE

```
### baseline job count
(7,)
('always_fails',) ('archive_old',) ('cleanup_logs',) ('extract_data',)
('load_data',) ('slow_job',) ('transform_data',)

### job count after
(3,)
('always_fails',) ('extract_data',) ('slow_job',)

WARNING - Skipping file .../jobs/etl.yaml: duplicate job name
'extract_data' (already defined in .../jobs/dup.yaml)
```

Worse than the audit recorded. A two-line file took the job count from 7 to
3: `etl.yaml` was discarded whole, `cleanup_logs` and `archive_old`
cascaded out behind it, and the surviving `extract_data` is the *intruder's*
definition, not the real one. The blast radius of a one-name collision was
four unrelated jobs plus a silent substitution of the wrong command.
