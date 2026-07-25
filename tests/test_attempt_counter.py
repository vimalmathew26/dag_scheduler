"""B2: the retry attempt counter must survive the re-enqueue round trip.

RetryEngine passed `attempt` to Scheduler.enqueue_job, whose signature
accepted it and never read it. The job went back to QUEUED with no memory
of the attempt number and was dispatched with the default of 1, so
should_retry always compared 1 against max_attempts. Observed: 47
executions in 20 seconds for a job configured for 3 attempts.
"""

import pytest

from dag_scheduler.models import JobDefinition, JobState


class FakeRegistry:
    def __init__(self, jobs):
        self.jobs = jobs

    def get_job(self, name):
        return self.jobs.get(name)

    def get_all_jobs(self):
        return dict(self.jobs)

    def known_job_names(self):
        return set(self.jobs)


@pytest.fixture
def scheduler(persistence):
    from dag_scheduler.scheduler import Scheduler

    jobs = {"j": JobDefinition(command="exit 1")}
    return Scheduler(persistence, FakeRegistry(jobs), executor=None)


class TestAttemptPersistence:
    async def test_fresh_job_starts_at_attempt_one(self, persistence, scheduler):
        await persistence.upsert_job("j", JobDefinition(command="exit 1"))
        await scheduler.enqueue_job("j", bypass_deps=True)
        assert await persistence.get_job_attempt("j") == 1

    async def test_enqueue_records_the_attempt_it_was_given(
        self, persistence, scheduler
    ):
        await persistence.upsert_job("j", JobDefinition(command="exit 1"))
        await scheduler.enqueue_job("j", bypass_deps=True, attempt=3)
        assert await persistence.get_job_attempt("j") == 3

    async def test_attempt_survives_the_claim(self, persistence, scheduler):
        await persistence.upsert_job("j", JobDefinition(command="exit 1"))
        await scheduler.enqueue_job("j", bypass_deps=True, attempt=2)

        claimed = await persistence.claim_next_queued_job()

        assert claimed == "j"
        assert await persistence.get_job_attempt("j") == 2

    async def test_retry_round_trip_increments_rather_than_resetting(
        self, persistence, scheduler
    ):
        """The exact round trip that was broken.

        Enqueue at attempt N, claim it, then enqueue again at N+1 the way
        RetryEngine._retry_after_delay does.
        """
        await persistence.upsert_job("j", JobDefinition(command="exit 1"))

        seen = []
        for attempt in (1, 2, 3):
            await scheduler.enqueue_job("j", bypass_deps=True, attempt=attempt)
            await persistence.claim_next_queued_job()
            seen.append(await persistence.get_job_attempt("j"))
            await persistence.update_job_state("j", JobState.FAILED)

        assert seen == [1, 2, 3], "each dispatch must see its own attempt number"

    async def test_a_fresh_trigger_after_failure_resets_to_one(
        self, persistence, scheduler
    ):
        """A manual re-trigger is a new run, not a continuation."""
        await persistence.upsert_job("j", JobDefinition(command="exit 1"))
        await scheduler.enqueue_job("j", bypass_deps=True, attempt=3)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("j", JobState.FAILED)

        await scheduler.enqueue_job("j", bypass_deps=True)

        assert await persistence.get_job_attempt("j") == 1
