# Findings outside the remediation plan

Discovered while executing the plan. Recorded, not fixed, per the scope lock.

| # | Location | Finding |
|---|---|---|
| F1 | `persistence.py:41-42` | `FAILED -> QUEUED` and `FAILED -> WAITING` are in `VALID_TRANSITIONS`, but the live retry path (`scheduler.py:86`) resets to `DEFINED` first and never uses them. They appear to be a second, unused route. Pinned by a test so the set is not trimmed unknowingly. |
| F2 | `definition_parser.py:76-88` | A job that is not a dict, or is missing `command`, still discards its entire file via `file_valid = False; break`. DECISION 1 made name collisions per-job, but these two structural checks remain per-file. The same rationale would apply. Left alone as outside the plan's scope. |
| F3 | `persistence.py` `age_queued_priorities` / `scheduler.py` `_aging_loop` | Priority aging increments *every* queued job by 1, so the gap between any two jobs is invariant and the ordering never changes. A starved low-priority job can never overtake a newer high-priority one, which is the entire purpose of aging. Found by a test that assumed it worked. The feature is effectively inert. Not fixed: fixing it means choosing a new aging policy, which is a design decision outside this plan. |
| F4 | `scheduler.py` `enqueue_job`, the no-definition branch | A job present in the database but absent from the registry is sent straight to BLOCKED_UNRESOLVABLE. That is legal from QUEUED but not from DEFINED or WAITING, so enqueueing a DEFINED job with no definition raises InvalidTransitionError rather than blocking it. Unreachable through the API, which 404s on an unknown job first, but reachable from a retry whose definition is removed during the backoff. Pinned by a test that documents the raise. |
| F5 | `file_watcher.py` `_debounce_reload` finally clause | Debouncing does not coalesce. `_handle_event` cancels the previous task and registers the new one, but the cancelled task's `finally` then pops the dict entry unconditionally, deregistering its own replacement. The next event therefore sees no registered task, creates another, and the orphaned one still fires. Measured: five edits 20ms apart with a 200ms debounce produce three reloads, not one. README.md line 130 claims edits are coalesced. Fix is to pop only when the registered task is self. |

