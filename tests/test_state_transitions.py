"""Exhaustive validation of the job state machine.

`Persistence.VALID_TRANSITIONS` is the authoritative definition of which
state changes are legal.  This module asserts that definition is complete
and that it has the shape the rest of the system relies on.

`validate_transition` performs no I/O, so none of this needs an event
loop.  It was made synchronous for that reason.
"""

import itertools

import pytest

from dag_scheduler.models import JobState
from dag_scheduler.persistence import InvalidTransitionError, Persistence

ALL_STATES = list(JobState)
ALL_PAIRS = list(itertools.product(ALL_STATES, ALL_STATES))

TERMINAL_STATES = {
    JobState.DONE,
    JobState.FAILED,
    JobState.TIMED_OUT,
    JobState.UNKNOWN,
    JobState.CANCELLED,
}


@pytest.fixture
def persistence(tmp_path):
    # validate_transition does no I/O; the path is never opened.
    return Persistence(tmp_path / "unused.db")


class TestExhaustiveMatrix:
    """Every one of the 100 ordered pairs, with no exceptions carved out."""

    def test_the_matrix_is_the_size_we_think_it_is(self):
        assert len(ALL_STATES) == 10
        assert len(ALL_PAIRS) == 100
        same_state = [(a, b) for a, b in ALL_PAIRS if a == b]
        assert len(same_state) == 10
        assert len(Persistence.VALID_TRANSITIONS) == 21
        illegal = len(ALL_PAIRS) - len(Persistence.VALID_TRANSITIONS) - len(same_state)
        assert illegal == 69

    @pytest.mark.parametrize("from_state,to_state", ALL_PAIRS)
    def test_membership_exactly_predicts_rejection(self, persistence, from_state, to_state):
        """Membership in VALID_TRANSITIONS decides acceptance, nothing else.

        A same-state transition is a documented no-op (persistence.py
        returns early), so it is permitted regardless of membership.
        """
        legal = (from_state, to_state) in Persistence.VALID_TRANSITIONS
        no_op = from_state == to_state

        if legal or no_op:
            persistence.validate_transition("j", from_state, to_state)
        else:
            with pytest.raises(InvalidTransitionError) as exc:
                persistence.validate_transition("j", from_state, to_state)
            assert exc.value.from_state == from_state
            assert exc.value.to_state == to_state
            assert exc.value.job_name == "j"

    @pytest.mark.parametrize("state", ALL_STATES)
    def test_same_state_is_always_a_no_op(self, persistence, state):
        persistence.validate_transition("j", state, state)


class TestStateMachineProperties:
    """Assertions about the *shape* of the machine.

    A membership test alone would happily accept a future edit that adds a
    plausible-looking but wrong entry to the set.  These would not.
    """

    def test_running_is_only_reachable_from_queued(self):
        sources = {f for f, t in Persistence.VALID_TRANSITIONS if t is JobState.RUNNING}
        assert sources == {JobState.QUEUED}

    def test_done_only_leads_to_defined(self):
        targets = {t for f, t in Persistence.VALID_TRANSITIONS if f is JobState.DONE}
        assert targets == {JobState.DEFINED}

    @pytest.mark.parametrize("terminal", sorted(TERMINAL_STATES, key=lambda s: s.value))
    def test_every_terminal_state_can_reset_to_defined(self, terminal):
        assert (terminal, JobState.DEFINED) in Persistence.VALID_TRANSITIONS

    def test_waiting_can_become_unresolvable(self):
        """Added with DECISION 2: a waiting job whose dependency's
        definition disappears is blocked rather than deleted."""
        assert (
            JobState.WAITING,
            JobState.BLOCKED_UNRESOLVABLE,
        ) in Persistence.VALID_TRANSITIONS

    def test_defined_is_never_a_terminal_dead_end(self):
        targets = {t for f, t in Persistence.VALID_TRANSITIONS if f is JobState.DEFINED}
        assert targets == {JobState.QUEUED, JobState.WAITING}

    def test_nothing_transitions_into_itself_in_the_table(self):
        assert not any(f is t for f, t in Persistence.VALID_TRANSITIONS)

    @pytest.mark.parametrize(
        "terminal",
        sorted(TERMINAL_STATES - {JobState.FAILED}, key=lambda s: s.value),
    )
    def test_terminal_states_leave_only_via_reset(self, terminal):
        targets = {t for f, t in Persistence.VALID_TRANSITIONS if f is terminal}
        assert targets == {JobState.DEFINED}, (
            f"{terminal.value} must only leave via a reset to defined"
        )

    def test_failed_is_the_one_terminal_state_with_a_direct_re_enqueue(self):
        """FAILED is deliberately special, and it is the only exception.

        A retry needs to put a failed job back in the queue.  The live
        retry path goes through Scheduler.enqueue_job, which resets to
        DEFINED first, so these two edges are a second, more direct route
        to the same place.  Pinned here so that if the set is ever
        trimmed, the decision is made knowingly.
        """
        targets = {t for f, t in Persistence.VALID_TRANSITIONS if f is JobState.FAILED}
        assert targets == {JobState.DEFINED, JobState.QUEUED, JobState.WAITING}

    def test_cancellation_is_reachable_from_queued_and_running_only(self):
        sources = {f for f, t in Persistence.VALID_TRANSITIONS if t is JobState.CANCELLED}
        assert sources == {JobState.QUEUED, JobState.RUNNING}

    def test_every_state_is_reachable_from_defined(self):
        """No state should be orphaned from the entry point."""
        adjacency = {}
        for f, t in Persistence.VALID_TRANSITIONS:
            adjacency.setdefault(f, set()).add(t)

        seen = {JobState.DEFINED}
        frontier = [JobState.DEFINED]
        while frontier:
            for nxt in adjacency.get(frontier.pop(), ()):
                if nxt not in seen:
                    seen.add(nxt)
                    frontier.append(nxt)

        assert seen == set(ALL_STATES), f"unreachable: {set(ALL_STATES) - seen}"

    def test_validate_transition_is_not_a_coroutine(self, persistence):
        result = persistence.validate_transition("j", JobState.DEFINED, JobState.QUEUED)
        assert result is None
