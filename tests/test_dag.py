"""Pure-function tests for the dependency graph.

Nothing here touches a database, an event loop, or the filesystem.
These were written before any production change, as the safety net for
the dispatch-path work that follows.
"""

import pytest

from dag_scheduler.dag import (
    CycleError,
    get_dependents,
    get_ready_jobs,
    topological_sort,
)
from dag_scheduler.models import JobDefinition, JobState


def job(*deps: str) -> JobDefinition:
    """A minimal JobDefinition with the given dependencies."""
    return JobDefinition(command="true", depends_on=list(deps))


def graph(**spec: tuple) -> dict:
    """Build a job dict from ``name=(dep, dep, ...)`` keyword pairs."""
    return {name: job(*deps) for name, deps in spec.items()}


def assert_valid_topological_order(order: list, jobs: dict) -> None:
    """Assert `order` is *a* valid topological order for `jobs`.

    Kahn's algorithm pops from a queue seeded by iterating a set, so the
    exact order is not deterministic across runs.  Asserting one specific
    list produces a flaky test; assert the invariant instead.
    """
    assert sorted(order) == sorted(jobs), "every job must appear exactly once"
    position = {name: i for i, name in enumerate(order)}
    for name, definition in jobs.items():
        for dep in definition.depends_on:
            if dep in jobs:
                assert position[dep] < position[name], (
                    f"{dep} must be ordered before its dependent {name}"
                )


class TestTopologicalSort:
    def test_empty_graph(self):
        assert topological_sort({}) == []

    def test_single_node(self):
        jobs = graph(a=())
        assert topological_sort(jobs) == ["a"]

    def test_linear_chain(self):
        jobs = graph(a=(), b=("a",), c=("b",))
        assert topological_sort(jobs) == ["a", "b", "c"]

    def test_diamond(self):
        # a -> b, a -> c, b -> d, c -> d
        jobs = graph(a=(), b=("a",), c=("a",), d=("b", "c"))
        assert_valid_topological_order(topological_sort(jobs), jobs)

    def test_node_with_three_parents(self):
        jobs = graph(p1=(), p2=(), p3=(), child=("p1", "p2", "p3"))
        order = topological_sort(jobs)
        assert_valid_topological_order(order, jobs)
        assert order[-1] == "child"

    def test_disconnected_components(self):
        jobs = graph(a=(), b=("a",), x=(), y=("x",))
        order = topological_sort(jobs)
        assert_valid_topological_order(order, jobs)
        assert len(order) == 4

    def test_dependency_absent_from_snapshot_is_skipped(self):
        # dag.py:32 only counts dependencies present in the snapshot.
        # This must not raise and must not leave `b` unreachable.
        jobs = graph(b=("missing_parent",))
        assert topological_sort(jobs) == ["b"]

    def test_dependency_absent_does_not_affect_in_degree(self):
        jobs = graph(a=(), b=("a", "not_here"))
        assert topological_sort(jobs) == ["a", "b"]

    def test_self_loop_raises(self):
        jobs = graph(a=("a",))
        with pytest.raises(CycleError):
            topological_sort(jobs)

    def test_two_node_cycle_raises(self):
        jobs = graph(a=("b",), b=("a",))
        with pytest.raises(CycleError):
            topological_sort(jobs)

    def test_three_node_cycle_raises(self):
        jobs = graph(a=("b",), b=("c",), c=("a",))
        with pytest.raises(CycleError):
            topological_sort(jobs)

    def test_cycle_alongside_acyclic_component_still_raises(self):
        jobs = graph(a=("b",), b=("a",), x=(), y=("x",))
        with pytest.raises(CycleError):
            topological_sort(jobs)

    def test_cycle_error_carries_the_cycle(self):
        jobs = graph(a=("b",), b=("a",))
        with pytest.raises(CycleError) as exc:
            topological_sort(jobs)
        assert exc.value.cycle, "CycleError must carry the offending path"
        assert set(exc.value.cycle) <= set(jobs)


class TestFindCycle:
    """_find_cycle is exercised through the CycleError it produces."""

    def _cycle_for(self, jobs):
        with pytest.raises(CycleError) as exc:
            topological_sort(jobs)
        return exc.value.cycle

    def test_reported_path_closes_on_itself(self):
        cycle = self._cycle_for(graph(a=("b",), b=("a",)))
        assert cycle[0] == cycle[-1], "a reported cycle must return to its start"

    def test_reported_path_edges_are_real_dependencies(self):
        jobs = graph(a=("b",), b=("c",), c=("a",))
        cycle = self._cycle_for(jobs)
        # dag.py walks depends_on, so cycle[i] depends on cycle[i + 1].
        for current, nxt in zip(cycle, cycle[1:], strict=False):
            assert nxt in jobs[current].depends_on

    def test_self_loop_path(self):
        cycle = self._cycle_for(graph(a=("a",)))
        assert cycle == ["a", "a"]

    def test_absent_nodes_are_ignored_when_walking(self):
        # A missing node cannot participate in a cycle, so skipping it at
        # dag.py:76 is correct.  Assert the real cycle is still found when
        # a missing dependency is also present on a cycle member.
        jobs = graph(a=("b", "ghost"), b=("a",))
        cycle = self._cycle_for(jobs)
        assert "ghost" not in cycle


class TestGetDependents:
    def test_no_dependents(self):
        assert get_dependents("a", graph(a=(), b=())) == set()

    def test_single_dependent(self):
        assert get_dependents("a", graph(a=(), b=("a",))) == {"b"}

    def test_multiple_dependents(self):
        jobs = graph(a=(), b=("a",), c=("a",), d=("b",))
        assert get_dependents("a", jobs) == {"b", "c"}

    def test_is_direct_only_not_transitive(self):
        jobs = graph(a=(), b=("a",), c=("b",))
        assert get_dependents("a", jobs) == {"b"}

    def test_unknown_job_has_no_dependents(self):
        assert get_dependents("nobody", graph(a=(), b=("a",))) == set()


class TestGetReadyJobs:
    """Dependency fan-out, including the multiple-parent case."""

    def test_single_parent_done_unblocks_child(self):
        jobs = graph(parent=(), child=("parent",))
        states = {"parent": JobState.DONE, "child": JobState.WAITING}
        assert get_ready_jobs("parent", jobs, states) == ["child"]

    def test_three_parents_one_done_is_not_ready(self):
        jobs = graph(p1=(), p2=(), p3=(), child=("p1", "p2", "p3"))
        states = {
            "p1": JobState.DONE,
            "p2": JobState.WAITING,
            "p3": JobState.WAITING,
            "child": JobState.WAITING,
        }
        assert get_ready_jobs("p1", jobs, states) == []

    def test_three_parents_two_done_is_not_ready(self):
        jobs = graph(p1=(), p2=(), p3=(), child=("p1", "p2", "p3"))
        states = {
            "p1": JobState.DONE,
            "p2": JobState.DONE,
            "p3": JobState.RUNNING,
            "child": JobState.WAITING,
        }
        assert get_ready_jobs("p2", jobs, states) == []

    def test_three_parents_all_done_is_ready(self):
        jobs = graph(p1=(), p2=(), p3=(), child=("p1", "p2", "p3"))
        states = {
            "p1": JobState.DONE,
            "p2": JobState.DONE,
            "p3": JobState.DONE,
            "child": JobState.WAITING,
        }
        assert get_ready_jobs("p3", jobs, states) == ["child"]

    @pytest.mark.parametrize(
        "parent_state",
        [
            JobState.FAILED,
            JobState.TIMED_OUT,
            JobState.UNKNOWN,
            JobState.CANCELLED,
            JobState.RUNNING,
            JobState.QUEUED,
            JobState.BLOCKED_UNRESOLVABLE,
        ],
    )
    def test_non_done_parent_never_unblocks(self, parent_state):
        jobs = graph(p1=(), p2=(), child=("p1", "p2"))
        states = {"p1": JobState.DONE, "p2": parent_state, "child": JobState.WAITING}
        assert get_ready_jobs("p1", jobs, states) == []

    def test_parent_missing_from_states_is_not_ready(self):
        jobs = graph(p1=(), p2=(), child=("p1", "p2"))
        states = {"p1": JobState.DONE, "child": JobState.WAITING}
        assert get_ready_jobs("p1", jobs, states) == []

    def test_fan_out_to_several_children(self):
        jobs = graph(parent=(), c1=("parent",), c2=("parent",))
        states = {
            "parent": JobState.DONE,
            "c1": JobState.WAITING,
            "c2": JobState.WAITING,
        }
        assert sorted(get_ready_jobs("parent", jobs, states)) == ["c1", "c2"]

    def test_completed_job_with_no_dependents(self):
        jobs = graph(a=(), b=())
        assert get_ready_jobs("a", jobs, {"a": JobState.DONE}) == []
