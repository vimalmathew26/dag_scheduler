"""Process-local counters exposed in Prometheus text format.

/stats answers "what does the database say happened". It cannot answer
"is the scheduler stuck", because queue depth, dispatch rate and
concurrency saturation are not rows in a table. These are.

Deliberately dependency-free and deliberately small: five counters and two
gauges, incremented in place.
"""

from collections import defaultdict

_counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
_gauges: dict[str, float] = {}

_HELP = {
    "dag_dispatches_total": ("counter", "Jobs claimed and handed to the executor"),
    "dag_runs_total": ("counter", "Job runs that reached a terminal state"),
    "dag_retries_total": ("counter", "Retries scheduled after a failure"),
    "dag_reload_failures_total": ("counter", "Definition reloads that raised"),
    "dag_scheduler_loop_errors_total": ("counter", "Unhandled errors in the dispatch loop"),
    "dag_running_jobs": ("gauge", "Jobs currently executing"),
    "dag_queued_jobs": ("gauge", "Jobs currently queued"),
}


def increment(name: str, value: float = 1.0, **labels: str) -> None:
    key = (name, tuple(sorted(labels.items())))
    _counters[key] += value


def set_gauge(name: str, value: float) -> None:
    _gauges[name] = value


def reset() -> None:
    """Clear all series. For tests."""
    _counters.clear()
    _gauges.clear()


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in labels)
    return "{" + inner + "}"


def render() -> str:
    """Render every series in Prometheus text exposition format."""
    by_name: dict[str, list] = defaultdict(list)
    for (name, labels), value in _counters.items():
        by_name[name].append((labels, value))
    for name, value in _gauges.items():
        by_name[name].append(((), value))

    # Emit declared series even at zero, so a scrape is never ambiguous
    # about whether a counter is absent or genuinely zero.
    for name in _HELP:
        by_name.setdefault(name, [((), 0.0)])

    lines = []
    for name in sorted(by_name):
        kind, help_text = _HELP.get(name, ("counter", name))
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        for labels, value in sorted(by_name[name]):
            lines.append(f"{name}{_format_labels(labels)} {value}")
    return "\n".join(lines) + "\n"
