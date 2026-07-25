# Findings outside the remediation plan

Discovered while executing the plan. Recorded, not fixed, per the scope lock.

| # | Location | Finding |
|---|---|---|
| F1 | `persistence.py:41-42` | `FAILED -> QUEUED` and `FAILED -> WAITING` are in `VALID_TRANSITIONS`, but the live retry path (`scheduler.py:86`) resets to `DEFINED` first and never uses them. They appear to be a second, unused route. Pinned by a test so the set is not trimmed unknowingly. |
