"""B1: dispatch must claim a job exactly once.

The scheduler used to select the highest-priority QUEUED row and dispatch
it without marking it taken, then loop again immediately. Observed: one
trigger of extract_data produced 22 subprocess executions.

These tests target the persistence primitive that makes that impossible.
"""

import asyncio

from dag_scheduler.models import JobDefinition, JobState
from dag_scheduler.persistence import InvalidTransitionError


async def queue_job(persistence, name, priority=1):
    await persistence.upsert_job(name, JobDefinition(command="true", priority=priority))
    await persistence.update_job_state(name, JobState.QUEUED)


class TestConcurrentTransition:
    async def test_concurrent_queued_to_running_has_exactly_one_winner(self, persistence):
        """The read-modify-write in update_job_state is not a transaction.

        Two callers can both read 'queued', both validate, and both write
        'running'. This is the mechanism by which duplicate dispatch
        bypassed the state machine entirely.
        """
        await queue_job(persistence, "j")

        results = await asyncio.gather(
            persistence.update_job_state("j", JobState.RUNNING),
            persistence.update_job_state("j", JobState.RUNNING),
            persistence.update_job_state("j", JobState.RUNNING),
            persistence.update_job_state("j", JobState.RUNNING),
            return_exceptions=True,
        )

        rejected = [r for r in results if isinstance(r, InvalidTransitionError)]
        assert len(rejected) == 3, (
            f"exactly one transition into RUNNING may succeed; {4 - len(rejected)} succeeded"
        )


class TestClaimNextQueuedJob:
    async def test_claims_and_marks_running_in_one_step(self, persistence):
        await queue_job(persistence, "j")

        claimed = await persistence.claim_next_queued_job()

        assert claimed == "j"
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.RUNNING

    async def test_returns_none_when_queue_is_empty(self, persistence):
        assert await persistence.claim_next_queued_job() is None

    async def test_ignores_jobs_that_are_not_queued(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        assert await persistence.claim_next_queued_job() is None

    async def test_concurrent_claims_of_one_job_yield_one_winner(self, persistence):
        """The regression test for B1.

        Twenty simultaneous claims against a single queued job must
        produce exactly one job name and nineteen Nones.
        """
        await queue_job(persistence, "only_one")

        results = await asyncio.gather(*(persistence.claim_next_queued_job() for _ in range(20)))

        winners = [r for r in results if r is not None]
        assert winners == ["only_one"], f"claimed {len(winners)} times, expected 1"

    async def test_concurrent_claims_never_hand_out_the_same_job_twice(self, persistence):
        for i in range(5):
            await queue_job(persistence, f"j{i}")

        results = await asyncio.gather(*(persistence.claim_next_queued_job() for _ in range(20)))

        claimed = [r for r in results if r is not None]
        assert sorted(claimed) == ["j0", "j1", "j2", "j3", "j4"]
        assert len(claimed) == len(set(claimed)), "a job was claimed twice"

    async def test_respects_priority_order(self, persistence):
        await queue_job(persistence, "low", priority=1)
        await queue_job(persistence, "high", priority=9)
        await queue_job(persistence, "mid", priority=5)

        order = [
            await persistence.claim_next_queued_job(),
            await persistence.claim_next_queued_job(),
            await persistence.claim_next_queued_job(),
        ]
        assert order == ["high", "mid", "low"]
