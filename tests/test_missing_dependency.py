"""B7 / DECISION 2: a dependency that no longer exists is unsatisfiable.

dag.get_ready_jobs filtered absent parents out of its check, so a job whose
only parent had been deleted was treated as having no preconditions and
became instantly ready. Persistence.revalidate_jobs took the opposite view
of the same question and moved such a job to BLOCKED_UNRESOLVABLE. Two
components disagreed; dag.py is the one that was wrong.

"depends_on: [B]" asserts this job must not run until B succeeds. If B is
absent that condition is unsatisfiable, not satisfied. A job declared with
no dependencies has no precondition; a job whose dependency vanished has an
unknowable one, and those must not be the same state.
"""

import pytest

from dag_scheduler.dag import get_ready_jobs, topological_sort
from dag_scheduler.models import JobDefinition, JobState


def job(*deps):
    return JobDefinition(command="true", depends_on=list(deps))


class TestGetReadyJobsWithMissingDependency:
    def test_job_whose_only_parent_vanished_is_not_ready(self):
        jobs = {"orphan": job("gone")}
        states = {"orphan": JobState.WAITING, "gone": JobState.DONE}

        assert get_ready_jobs("gone", jobs, states) == []

    def test_one_present_done_parent_does_not_excuse_a_missing_one(self):
        jobs = {"present": job(), "child": job("present", "gone")}
        states = {
            "present": JobState.DONE,
            "child": JobState.WAITING,
            "gone": JobState.DONE,
        }

        assert get_ready_jobs("present", jobs, states) == [], (
            "a missing parent must block, even when every present parent is done"
        )

    def test_a_job_with_no_dependencies_is_still_ready(self):
        """No precondition is not the same as an unknowable one."""
        jobs = {"parent": job(), "child": job("parent")}
        states = {"parent": JobState.DONE, "child": JobState.WAITING}

        assert get_ready_jobs("parent", jobs, states) == ["child"]


class TestComponentsAgree:
    """Both paths must reach the same conclusion about the same job."""

    async def test_dag_and_revalidate_agree_a_missing_parent_blocks(
        self, persistence
    ):
        await persistence.upsert_job("child", job("gone"))
        await persistence.update_job_state("child", JobState.WAITING)

        # Path 1: the graph refuses to call it ready.
        snapshot = {"child": job("gone")}
        assert get_ready_jobs("gone", snapshot, {"child": JobState.WAITING}) == []

        # Path 2: revalidation moves it to blocked.
        await persistence.revalidate_jobs(snapshot)

        jobs = await persistence.get_all_db_jobs()
        assert jobs["child"]["state"] is JobState.BLOCKED_UNRESOLVABLE

    async def test_removed_dependency_leaves_the_dependent_blocked_not_deleted(
        self, persistence
    ):
        """A waiting job whose definition disappears is blocked, not erased."""
        await persistence.upsert_job("child", job("parent"))
        await persistence.update_job_state("child", JobState.WAITING)

        await persistence.handle_removed_job("child")

        jobs = await persistence.get_all_db_jobs()
        assert "child" in jobs, "a waiting job must not silently vanish"
        assert jobs["child"]["state"] is JobState.BLOCKED_UNRESOLVABLE


class TestCycleDetectionStillIgnoresAbsentNodes:
    """Deliberately unchanged. An absent node cannot be part of a cycle.

    topological_sort and _find_cycle skip dependencies that are not in the
    snapshot. That filtering is correct for their purpose: they answer
    "is there a cycle among these nodes", and a name with no node cannot
    close a loop. Only the readiness predicate was wrong.
    """

    def test_absent_dependency_does_not_prevent_ordering(self):
        jobs = {"a": job(), "b": job("a", "not_here")}
        assert topological_sort(jobs) == ["a", "b"]

    def test_absent_dependency_is_not_reported_as_a_cycle(self):
        jobs = {"a": job("ghost")}
        assert topological_sort(jobs) == ["a"]
