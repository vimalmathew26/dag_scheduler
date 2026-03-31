from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class JobState(str, Enum):
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
    retry_on_exit_codes: List[int] = Field(default_factory=lambda: [1])


class JobDefinition(BaseModel):
    command: str
    depends_on: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    priority: int = 1
    timeout: int = 60
    retry: RetryPolicy = Field(default_factory=RetryPolicy)


class JobRun(BaseModel):
    job_name: str
    run_id: str
    state: JobState
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    exit_code: Optional[int] = None
    attempt: int = 1


class DefinitionFile(BaseModel):
    jobs: Dict[str, JobDefinition]
