"""Pure-function tests for retry eligibility and backoff.

`should_retry` and `calculate_backoff` perform no I/O, so these need no
event loop.  `calculate_backoff` was made synchronous for exactly that
reason; it was declared async but never awaited anything.
"""

import random

import pytest

from dag_scheduler.models import JobDefinition, JobRun, JobState, RetryPolicy
from dag_scheduler.retry_engine import RetryEngine


@pytest.fixture
def engine() -> RetryEngine:
    # should_retry and calculate_backoff never touch the scheduler.
    return RetryEngine(scheduler=None)


def definition(**policy) -> JobDefinition:
    return JobDefinition(command="true", retry=RetryPolicy(**policy))


def run(attempt: int) -> JobRun:
    return JobRun(job_name="j", run_id="r", state=JobState.FAILED, attempt=attempt)


class TestShouldRetry:
    @pytest.mark.parametrize(
        "attempt,max_attempts,expected",
        [
            (1, 3, True),
            (2, 3, True),
            (3, 3, False),
            (4, 3, False),
            (1, 1, False),
            (1, 0, False),
        ],
    )
    def test_attempt_against_max_attempts(self, engine, attempt, max_attempts, expected):
        d = definition(max_attempts=max_attempts, retry_on_exit_codes=[1])
        assert engine.should_retry(d, run(attempt), 1) is expected

    def test_max_attempts_one_never_retries(self, engine):
        d = definition(max_attempts=1, retry_on_exit_codes=[1, 2, 3])
        assert engine.should_retry(d, run(1), 1) is False

    @pytest.mark.parametrize(
        "exit_code,codes,expected",
        [
            (1, [1], True),
            (2, [1], False),
            (2, [1, 2], True),
            (0, [1], False),
            (0, [0], True),
            (127, [1], False),
        ],
    )
    def test_exit_code_against_policy(self, engine, exit_code, codes, expected):
        d = definition(max_attempts=5, retry_on_exit_codes=codes)
        assert engine.should_retry(d, run(1), exit_code) is expected

    def test_signal_exit_code_not_retried_by_default(self, engine):
        # The executor records -15 when it SIGTERMs a process.  The default
        # policy lists only [1], so a killed job must not be retried.
        d = definition(max_attempts=5)
        assert engine.should_retry(d, run(1), -15) is False

    def test_signal_exit_code_retried_when_policy_lists_it(self, engine):
        d = definition(max_attempts=5, retry_on_exit_codes=[-15])
        assert engine.should_retry(d, run(1), -15) is True

    def test_exhausted_attempts_beat_a_matching_exit_code(self, engine):
        d = definition(max_attempts=2, retry_on_exit_codes=[1])
        assert engine.should_retry(d, run(2), 1) is False

    def test_empty_retry_code_list_never_retries(self, engine):
        d = definition(max_attempts=5, retry_on_exit_codes=[])
        assert engine.should_retry(d, run(1), 1) is False


class TestCalculateBackoff:
    @pytest.mark.parametrize("attempt", [1, 2, 3, 4, 5])
    def test_exponential_without_jitter_is_exact(self, engine, attempt):
        d = definition(backoff_base=2.0, jitter=False)
        assert engine.calculate_backoff(d, attempt) == 2.0**attempt

    def test_base_one_is_constant(self, engine):
        d = definition(backoff_base=1.0, jitter=False)
        assert all(engine.calculate_backoff(d, a) == 1.0 for a in range(1, 6))

    def test_fractional_base(self, engine):
        d = definition(backoff_base=1.5, jitter=False)
        assert engine.calculate_backoff(d, 2) == pytest.approx(2.25)

    def test_jitter_upper_bound(self, engine, monkeypatch):
        monkeypatch.setattr(random, "uniform", lambda lo, hi: hi)
        d = definition(backoff_base=2.0, jitter=True)
        assert engine.calculate_backoff(d, 3) == pytest.approx(8.0 * 1.2)

    def test_jitter_lower_bound(self, engine, monkeypatch):
        monkeypatch.setattr(random, "uniform", lambda lo, hi: lo)
        d = definition(backoff_base=2.0, jitter=True)
        assert engine.calculate_backoff(d, 3) == pytest.approx(8.0 * 0.8)

    def test_jitter_stays_within_twenty_percent(self, engine):
        d = definition(backoff_base=2.0, jitter=True)
        for _ in range(200):
            value = engine.calculate_backoff(d, 4)
            assert 16.0 * 0.8 <= value <= 16.0 * 1.2

    def test_never_negative(self, engine, monkeypatch):
        # Force a jitter draw more negative than the backoff itself.
        monkeypatch.setattr(random, "uniform", lambda lo, hi: -1000.0)
        d = definition(backoff_base=2.0, jitter=True)
        assert engine.calculate_backoff(d, 1) == 0

    def test_is_not_a_coroutine(self, engine):
        # It performs no I/O; keeping it async forced an event loop on
        # every caller and every test for no reason.
        d = definition(jitter=False)
        assert not hasattr(engine.calculate_backoff(d, 1), "__await__")
