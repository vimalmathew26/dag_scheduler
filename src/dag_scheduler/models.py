from enum import Enum

from pydantic import BaseModel, Field


class JobState(str, Enum):  # noqa: UP042
    """Job lifecycle states.

    Inherits from str as well as Enum so members compare equal to the
    strings stored in SQLite and serialise directly through pydantic.
    StrEnum would be the modern spelling but changes __str__, which the
    stored values and the API responses depend on.
    """

    DEFINED = "defined"
    WAITING = "waiting"
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    UNKNOWN = "unknown"
    CANCELLED = "cancelled"
    BLOCKED_UNRESOLVABLE = "blocked_unresolvable"


class RetryPolicy(BaseModel):
    max_attempts: int = 3
    backoff_base: float = 2.0
    jitter: bool = True
    retry_on_exit_codes: list[int] = Field(default_factory=lambda: [1])


class JobDefinition(BaseModel):
    command: str
    depends_on: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    priority: int = 1
    timeout: int = 60
    retry: RetryPolicy = Field(default_factory=RetryPolicy)


class JobRun(BaseModel):
    job_name: str
    run_id: str
    state: JobState
    start_time: str | None = None
    end_time: str | None = None
    exit_code: int | None = None
    attempt: int = 1


class DefinitionFile(BaseModel):
    jobs: dict[str, JobDefinition]
