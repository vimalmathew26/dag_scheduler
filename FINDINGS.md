# Findings outside the remediation plan

Discovered while executing the plan. Recorded rather than fixed, per the
scope lock, unless noted otherwise.

| # | Location | Finding | Status |
|---|---|---|---|
| F1 | `persistence.py` `VALID_TRANSITIONS` | `FAILED -> QUEUED` and `FAILED -> WAITING` are in the table, but the live retry path resets to `DEFINED` first and never uses them. They appear to be a second, unused route. Pinned by a test so the set is not trimmed unknowingly. | Open |
| F2 | `definition_parser.py` pass 1 | A job that is not a dict, or is missing `command`, still discards its entire file. DECISION 1 made name collisions per-job, but these two structural checks remain per-file. The same rationale would apply. | Open |
| F3 | `persistence.age_queued_priorities` | Priority aging incremented every queued job by 1, so the gap between any two was invariant and the ordering never changed. A starved low-priority job could never overtake a newer high-priority one, which is the only thing aging exists to do. Found by a test that assumed it worked. | **Fixed** |
| F4 | `scheduler.enqueue_job`, the no-definition branch | A job in the database but absent from the registry is sent to BLOCKED_UNRESOLVABLE, which is legal from QUEUED but not from DEFINED or WAITING, so it raises instead of blocking. Unreachable through the API, which 404s first, but reachable from a retry whose definition is removed during backoff. Pinned by a test that documents the raise. | Open |
| F5 | `file_watcher._debounce_reload` | Debouncing did not coalesce. The cancelled task's `finally` deregistered its own replacement, so the next event created a third task while the orphaned second still fired. Five edits 20ms apart produced three reloads. | **Fixed** |
