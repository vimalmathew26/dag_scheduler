"""Persistence round trips, priority ordering, aging and reset."""

import pytest

from dag_scheduler.models import JobDefinition, JobRun, JobState
from dag_scheduler.persistence import InvalidTransitionError

TERMINAL = [
    JobState.DONE,
    JobState.FAILED,
    JobState.TIMED_OUT,
    JobState.UNKNOWN,
    JobState.CANCELLED,
]


async def to_state(persistence, name, state):
    """Walk a legal path from DEFINED to the requested state."""
    if state is JobState.DEFINED:
        return
    if state is JobState.WAITING:
        await persistence.update_job_state(name, JobState.WAITING)
        return
    await persistence.update_job_state(name, JobState.QUEUED)
    if state is JobState.QUEUED:
        return
    if state is JobState.BLOCKED_UNRESOLVABLE:
        await persistence.update_job_state(name, JobState.BLOCKED_UNRESOLVABLE)
        return
    await persistence.claim_next_queued_job()
    if state is not JobState.RUNNING:
        await persistence.update_job_state(name, state)


class TestUpsert:
    async def test_insert_then_read_back(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="echo hi", priority=4))
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.DEFINED
        assert jobs["j"]["definition"]["command"] == "echo hi"

    async def test_update_changes_definition_and_priority(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="old", priority=1))
        await persistence.upsert_job("j", JobDefinition(command="new", priority=8))
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["definition"]["command"] == "new"
        assert jobs["j"]["definition"]["priority"] == 8

    async def test_update_preserves_state(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="old"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.upsert_job("j", JobDefinition(command="new"))
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.QUEUED, (
            "editing a definition must not restart a job's lifecycle"
        )


class TestUpdateJobState:
    async def test_unknown_job_is_a_silent_no_op(self, persistence):
        await persistence.update_job_state("nope", JobState.QUEUED)

    async def test_illegal_transition_raises(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        with pytest.raises(InvalidTransitionError):
            await persistence.update_job_state("j", JobState.DONE)

    async def test_same_state_is_a_no_op(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.DEFINED)

    async def test_legal_chain(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("j", JobState.DONE)
        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.DONE


class TestQueueOrdering:
    async def test_priority_desc_then_creation_asc(self, persistence):
        for name, priority in [("a", 1), ("b", 5), ("c", 5), ("d", 9)]:
            await persistence.upsert_job(name, JobDefinition(command="true", priority=priority))
            await persistence.update_job_state(name, JobState.QUEUED)

        order = [await persistence.claim_next_queued_job() for _ in range(4)]
        assert order[0] == "d"
        assert set(order[1:3]) == {"b", "c"}
        assert order[3] == "a"

    async def test_eligibility_filter_excludes_unknown_names(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        assert await persistence.claim_next_queued_job({"other"}) is None
        assert await persistence.claim_next_queued_job({"j"}) == "j"

    async def test_empty_eligibility_claims_nothing(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        assert await persistence.claim_next_queued_job(set()) is None


class TestPriorityAging:
    async def test_only_queued_jobs_age(self, persistence):
        await persistence.upsert_job("q", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("q", JobState.QUEUED)
        await persistence.mark_queued_at("q", seconds_ago=300)
        await persistence.upsert_job("d", JobDefinition(command="true", priority=1))

        promoted = await persistence.age_queued_priorities(interval_seconds=60)

        assert promoted == 1
        assert await persistence.priority_of("q") == 2
        assert await persistence.priority_of("d") == 1

    async def test_a_starved_job_overtakes_a_newer_higher_priority_one(self, persistence):
        """The whole point of aging.

        This used to be impossible: every queued job was incremented by the
        same amount, so the gap between any two was invariant and the
        dispatch order never changed however long a job waited.
        """
        await persistence.upsert_job("old", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("old", JobState.QUEUED)
        await persistence.mark_queued_at("old", seconds_ago=600)

        await persistence.upsert_job("new", JobDefinition(command="true", priority=3))
        await persistence.update_job_state("new", JobState.QUEUED)

        for _ in range(3):
            await persistence.age_queued_priorities(interval_seconds=60)

        assert await persistence.claim_next_queued_job() == "old"

    async def test_a_freshly_queued_job_is_not_aged(self, persistence):
        await persistence.upsert_job("fresh", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("fresh", JobState.QUEUED)

        await persistence.age_queued_priorities(interval_seconds=60)

        assert await persistence.priority_of("fresh") == 1

    async def test_only_jobs_past_the_interval_are_aged(self, persistence):
        await persistence.upsert_job("waited", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("waited", JobState.QUEUED)
        await persistence.mark_queued_at("waited", seconds_ago=300)

        await persistence.upsert_job("fresh", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("fresh", JobState.QUEUED)

        await persistence.age_queued_priorities(interval_seconds=60)

        assert await persistence.priority_of("waited") == 2
        assert await persistence.priority_of("fresh") == 1

    async def test_only_queued_jobs_are_aged_at_all(self, persistence):
        await persistence.upsert_job("d", JobDefinition(command="true", priority=1))
        await persistence.age_queued_priorities(interval_seconds=0)
        assert await persistence.priority_of("d") == 1

    async def test_claiming_and_requeueing_restarts_the_clock(self, persistence):
        """A job that runs and is re-queued has not been waiting."""
        await persistence.upsert_job("j", JobDefinition(command="true", priority=1))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.mark_queued_at("j", seconds_ago=600)
        await persistence.claim_next_queued_job()
        await persistence.update_job_state("j", JobState.FAILED)
        await persistence.update_job_state("j", JobState.QUEUED)

        await persistence.age_queued_priorities(interval_seconds=60)

        assert await persistence.priority_of("j") == 1


class TestResetJobState:
    @pytest.mark.parametrize("state", TERMINAL)
    async def test_reset_from_every_terminal_state(self, persistence, state):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await to_state(persistence, "j", state)

        assert await persistence.reset_job_state("j") is True

        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.DEFINED

    @pytest.mark.parametrize(
        "state", [JobState.DEFINED, JobState.QUEUED, JobState.RUNNING, JobState.WAITING]
    )
    async def test_reset_from_a_non_terminal_state_raises(self, persistence, state):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await to_state(persistence, "j", state)
        with pytest.raises(InvalidTransitionError):
            await persistence.reset_job_state("j")

    async def test_reset_of_unknown_job_returns_false(self, persistence):
        assert await persistence.reset_job_state("nope") is False


class TestRuns:
    async def test_record_and_finalize(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.record_run(
            JobRun(
                job_name="j",
                run_id="r1",
                state=JobState.RUNNING,
                start_time="2026-01-01 00:00:00",
                attempt=2,
            )
        )
        await persistence.finalize_run("r1", JobState.DONE, 0)

        runs = await persistence.get_runs_for_job("j")
        assert len(runs) == 1
        assert runs[0]["state"] == "done"
        assert runs[0]["exit_code"] == 0
        assert runs[0]["attempt"] == 2
        assert runs[0]["end_time"] is not None

    async def test_finalize_accepts_a_null_exit_code(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.record_run(
            JobRun(
                job_name="j", run_id="r1", state=JobState.RUNNING, start_time="2026-01-01 00:00:00"
            )
        )
        await persistence.finalize_run("r1", JobState.UNKNOWN, None)
        runs = await persistence.get_runs_for_job("j")
        assert runs[0]["exit_code"] is None

    async def test_no_runs_for_a_job_that_never_ran(self, persistence):
        assert await persistence.get_runs_for_job("nope") == []


class TestRevalidate:
    async def test_waiting_job_with_missing_dependency_becomes_blocked(self, persistence):
        await persistence.upsert_job("child", JobDefinition(command="true", depends_on=["gone"]))
        await persistence.update_job_state("child", JobState.WAITING)

        await persistence.revalidate_jobs(
            {"child": JobDefinition(command="true", depends_on=["gone"])}
        )

        jobs = await persistence.get_all_db_jobs()
        assert jobs["child"]["state"] is JobState.BLOCKED_UNRESOLVABLE

    async def test_blocked_job_becomes_waiting_when_dependency_returns(self, persistence):
        await persistence.upsert_job("child", JobDefinition(command="true", depends_on=["parent"]))
        await persistence.update_job_state("child", JobState.WAITING)
        await persistence.update_job_state("child", JobState.BLOCKED_UNRESOLVABLE)

        snapshot = {
            "parent": JobDefinition(command="true"),
            "child": JobDefinition(command="true", depends_on=["parent"]),
        }
        await persistence.revalidate_jobs(snapshot)

        jobs = await persistence.get_all_db_jobs()
        assert jobs["child"]["state"] is JobState.WAITING

    async def test_running_jobs_are_untouched(self, persistence):
        await persistence.upsert_job("j", JobDefinition(command="true"))
        await persistence.update_job_state("j", JobState.QUEUED)
        await persistence.claim_next_queued_job()

        await persistence.revalidate_jobs({})

        jobs = await persistence.get_all_db_jobs()
        assert jobs["j"]["state"] is JobState.RUNNING
