"""Logging configuration and per-run correlation.

Log lines used to carry the run_id only inside one message string, in one
place, so there was no way to ask "show me everything about this run".
JobLogAdapter attaches job, run and attempt to every record the executor
and retry engine emit, and the JSON formatter makes those fields queryable
rather than something to grep out of prose.
"""

import json
import logging
import sys
from typing import Any, Dict, MutableMapping, Optional, Tuple

# Attributes present on every LogRecord, so anything else was added by us.
_STANDARD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message", "asctime", "taskName",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per line, including any adapter-supplied fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class JobLogAdapter(logging.LoggerAdapter):
    """Binds job, run and attempt to every record emitted through it."""

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> Tuple[Any, MutableMapping[str, Any]]:
        extra: Dict[str, Any] = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        run_id = extra.get("run_id")
        prefix = f"[{extra.get('job')}"
        if run_id:
            prefix += f" run={str(run_id)[:8]}"
        attempt = extra.get("attempt")
        if attempt is not None:
            prefix += f" attempt={attempt}"
        return f"{prefix}] {msg}", kwargs


def for_run(
    logger: logging.Logger,
    job: str,
    run_id: Optional[str] = None,
    attempt: Optional[int] = None,
) -> JobLogAdapter:
    extra: Dict[str, Any] = {"job": job}
    if run_id is not None:
        extra["run_id"] = run_id
    if attempt is not None:
        extra["attempt"] = attempt
    return JobLogAdapter(logger, extra)


def configure_logging(level: str = "INFO", json_output: bool = False) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if json_output:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())
