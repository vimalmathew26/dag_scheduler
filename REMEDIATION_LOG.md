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

### AFTER

```
### run count
(3,)
### distinct attempt numbers recorded
(1, 1)
(2, 1)
(3, 1)
### final job state
('failed',)
### did it ever conclude?
1
```

Exactly three runs, numbered 1, 2 and 3, then the job settles in `failed`
and the give-up branch is reached once.

### Fix

The attempt number is now a column on `jobs` rather than a parameter that
was dropped crossing the queue. `enqueue_job` writes `current_attempt`,
which it previously accepted and ignored; the dispatch loop reads it back
after claiming and passes it to the executor. A fresh trigger resets to 1,
so a manual re-run is a new run rather than a continuation. Databases
created before this change get the column by migration in `setup()`.

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

### AFTER

```
### job state (expect timed_out)
('timed_out',)
### orphaned 'sleep 30' processes alive 8s after a 2s timeout
0
### detail
### still alive after the daemon exits
0
```

### Fix

Two things were wrong, and only the first was in the audit.

The timeout was enforced by wrapping `_execute_shell` in `wait_for`, so a
timeout cancelled that coroutine and ran its `finally`, deregistering the
process. `_handle_timeout` then looked the process up and got None, making
the whole SIGTERM/SIGKILL block unreachable. The timeout is now enforced
inside `_execute_shell`, where the process handle is in scope, and returns
`(exit_code, timed_out)` rather than relying on an exception.

The second is that killing the direct child is not enough. A job runs under
a shell, so `sleep 30` is a grandchild; the before-capture shows both
`/bin/sh -c sleep 30` and `sleep 30` surviving. Jobs are now spawned with
`start_new_session=True` and signalled as a process group, so the whole job
dies rather than just its shell. `ProcessManager.terminate` owns that
escalation and is shared with the cancel path.

The test measures process group membership read from /proc rather than
matching command lines, because the surrounding harness has the job's
command text in its own argv and a `pgrep -f` check gave a false positive.

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

### AFTER

```
### job-level state
('cancelled',)
### run-level states (a cancelled run must not say timed_out)
('cancelled', -15, 0)
### unretrieved task exceptions
0
### illegal transitions attempted
(none)
```

One run row, correctly labelled `cancelled`, carrying the real SIGTERM
return code of -15 rather than an invented -1, with `end_time` set. No
illegal transitions and no crashed tasks.

### Fix, per DECISION 3

Cancellation is now something the executor checks for rather than
discovers by raising. After the process ends, `run_job` asks whether the
job was cancelled; if so it finalizes the run as CANCELLED with the real
return code and does not attempt to write its own outcome over a terminal
state. The same check guards the generic exception handler, which
previously called `update_job_state` and so could raise the very error
class it existed to absorb.

A job cancelled before it reaches a concurrency slot returns without
recording a run, because nothing started.

Exit code recording follows DECISION 3 exactly. Cancelled while running
records the actual return code of the killed process. Cancelled while
queued, where no process ever existed, records NULL for both `start_time`
and `exit_code` and sets `end_time`. No sentinel is invented for a process
that never ran.

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

### AFTER

```
### before SIGKILL
('running',)
('running', 1)
### job-level state after recovery (expect unknown)
('unknown',)
### run-level rows after recovery (expect none left in 'running')
('unknown', 1)
### dangling rows with no end_time
(0,)
```

Note the before-state also shows the B1 fix holding: one run row where
there used to be four.

### Fix

`finalize_orphaned_runs` closes out every run still marked RUNNING at
startup, setting state UNKNOWN and an end time. The exit code is left NULL
because nothing observed the process exit, which is the same reasoning that
gives the job-level UNKNOWN state its meaning. Recovery is idempotent and
leaves already-finalized runs alone.

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

### AFTER

```
### job count after
(2,)
('always_fails',) ('slow_job',)

WARNING - Rejecting all 2 definitions of job 'extract_data': declared in
multiple files (.../jobs/dup.yaml, .../jobs/etl.yaml). Remove the
duplicate and the job will load.
```

### Fix, per DECISION 1

Both definitions of the colliding name are rejected. The count is now 2
rather than 3, which is numerically lower than the before-state and is the
correct outcome: `extract_data` is genuinely ambiguous, so it does not
load at all, and the four jobs downstream of it cascade out because their
dependency is unresolvable. What is gone is the silent substitution. The
old behaviour resolved `extract_data` to `echo duplicate`, the intruder's
command, and would have run it.

The two survivors are exactly the jobs with no relationship to the
collision, which is the property DECISION 1 asks for. In the unit tests,
where the colliding files also carry unrelated jobs, every one of those
survives.

Which file used to win was decided by `directory.glob()` enumeration
order. Files are now parsed in sorted order, so the outcome is
reproducible, and a test asserts two parses of the same directory agree.

The warning names the job and every conflicting path, and says what to do
about it.

---

## B7 / DECISION 2: a deleted dependency must block its dependent

`dag.get_ready_jobs` filtered absent parents out of its readiness check
(`dag.py:134`), so a job whose only parent had been deleted was treated as
having no preconditions and became instantly ready.
`Persistence.revalidate_jobs` took the opposite view of the same question.

### Command

```bash
bash /tmp/repro/b7.sh
```

Creates `dep_parent.yaml` and `dep_child.yaml` where the child depends on
the parent, puts the child in WAITING, then deletes the parent's file.

### BEFORE

`get_ready_jobs` returned the dependent as ready, and the dependent's row
was deleted outright by `handle_removed_job` rather than reported as
blocked. Covered by four unit tests that failed before the change:

```
FAILED test_job_whose_only_parent_vanished_is_not_ready
FAILED test_one_present_done_parent_does_not_excuse_a_missing_one
FAILED test_dag_and_revalidate_agree_a_missing_parent_blocks
FAILED test_removed_dependency_leaves_the_dependent_blocked_not_deleted
```

### AFTER

```
### put dep_child in WAITING (its parent has not run)
('dep_child', 'waiting')
('dep_parent', 'defined')

### now delete the parent's definition file
### dependent must be blocked_unresolvable, not deleted, not run
('dep_child', 'blocked_unresolvable')
### did the child ever execute?
0
(0,)
```

### Fix

`get_ready_jobs` now treats a dependency absent from the snapshot as
unsatisfiable rather than satisfied, which is the same conclusion
`revalidate_jobs` already reached. A job with no dependencies is still
ready immediately; the two situations are no longer conflated.

`handle_removed_job` moves a WAITING job to BLOCKED_UNRESOLVABLE instead of
deleting it, so a dependent whose definition disappears is reported rather
than erased. That required adding WAITING to BLOCKED_UNRESOLVABLE to the
transition table, taking it from 20 legal transitions to 21, which the
exhaustive matrix test asserts.

### Cycle detection deliberately left alone

`topological_sort` (`dag.py:32`) and `_find_cycle` (`dag.py:76`) do the same
filtering and were not changed. Their question is "is there a cycle among
these nodes", and a name with no node cannot close a loop, so skipping
absent dependencies is correct for that purpose. Only the readiness
predicate was wrong. Two tests pin this so the distinction is deliberate.

---

# Final verification

From a clean `git clone` into a fresh virtualenv, installing with only the
commands the README gives, on a machine state with no prior database.

Environment caveat unchanged: CPython 3.10 with a `tomllib`/`tomli` shim,
because no 3.11+ interpreter could be obtained. 3.11, 3.12 and 3.13 are
covered by the CI matrix, which is the first place they are exercised.

## 1. Install following only the README

```bash
git clone <repo> && cd dag_scheduler
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Succeeded. `dag-scheduler --help` lists all nine commands.

## 2. Daemon starts, example definitions load

```
$ dag-scheduler daemon
$ dag-scheduler status
Job Status:
  extract_data                   defined
  transform_data                 defined
  load_data                      defined
  always_fails                   defined
  cleanup_logs                   defined
  archive_old                    defined
  slow_job                       defined
```

Seven jobs. `cycle.yaml`'s two are rejected at load, as documented.

## 3. Trigger extract_data once: exactly one run per job

```
('archive_old', 1)
('cleanup_logs', 1)
('extract_data', 1)
('load_data', 1)
('transform_data', 1)
```

Was 22 executions across the chain.

## 4. always_fails: exactly three attempts with visible backoff

```
  425cd58f  attempt=3  state=failed  exit=1  start=2026-07-25 13:30:20
  08d2ca5a  attempt=2  state=failed  exit=1  start=2026-07-25 13:30:18
  8d30a4ad  attempt=1  state=failed  exit=1  start=2026-07-25 13:30:16
('failed',)
```

Three runs, numbered, two seconds apart, then it stops. Was 47 and counting.

## 5. slow_job: TIMED_OUT, no orphans

```
  slow_job                       timed_out
orphaned sleep processes: 0
```

Was 4 processes surviving the daemon.

## 6. Cancel a running job

```
job level:  ('cancelled',)
run level:  ('cancelled', -15, 1)     # state, exit_code, end_time set
orphans after cancel: 0
```

Real SIGTERM return code per DECISION 3, not the old invented -1, and the
run says `cancelled` rather than `timed_out`.

## 7. Duplicate name: both rejected, everything else survives

```
extract_data present?  (0,)
unrelated jobs still here?
('always_fails',) ('slow_job',) ('vchild',) ('vparent',)

WARNING - Rejecting all 2 definitions of job 'extract_data': declared in
multiple files (.../jobs/dup.yaml, .../jobs/etl.yaml). Remove the duplicate
and the job will load.
```

Per DECISION 1. Was: the intruder's definition won and etl.yaml was
discarded whole.

## 8. Delete a dependency's definition file

```
before:  ('vchild', 'waiting')     ('vparent', 'defined')
after:   ('vchild', 'blocked_unresolvable')
child ever executed?  0
job_runs for vchild:  (0,)
```

Per DECISION 2. Was: the dependent became instantly ready and ran.

## 9. SIGKILL mid-run, restart

```
job state after restart:  ('unknown',)
runs still dangling in 'running':  (0,)
INFO - Crash recovery completed. Marked 1 jobs and 1 run(s) as unknown.
```

Was: job correct, 4 run rows stuck in `running` permanently.

## 10. Clean SIGTERM with a job running

```
daemon exited: YES
orphaned processes: 0
dangling run rows: (0,)
CancelledError tracebacks: 0
unretrieved task exceptions: 0
```

All four were wrong before.

## 11. Full test suite

461 passed, 86% coverage.

Weakest modules, and why: `file_watcher.py` 66% and `cli.py` 65% are
dominated by watchdog's threaded callbacks and Click's output formatting,
both of which are thin and churn-prone. `__main__.py` 61% is the uvicorn
serve path, exercised by the CI smoke job rather than in-process. The
modules carrying the logic are 92 to 100: persistence 98, definition_parser
99, executor 96, dag 95, scheduler 92, metrics and models 100.

## 12. Lint and type check

```
ruff check src tests        All checks passed!
ruff format --check         43 files already formatted
mypy                        Success: no issues found in 18 source files
```

mypy at `disallow_untyped_defs` and `check_untyped_defs`, configured in
pyproject. Was 98 errors at that setting.
