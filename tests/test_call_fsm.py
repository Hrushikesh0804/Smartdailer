"""
tests/test_call_fsm.py — Unit tests for the call finite-state machine.

Covers:
- All valid forward transitions (including skips).
- Terminal absorption: every event on every terminal state is rejected.
- Backward / same-rank rejection.
- UNKNOWN side-state semantics.
- apply_event() return value contract.
"""

import pytest
from smartdialer.domain.call_fsm import (
    CallState,
    STATE_RANK,
    TERMINAL_STATES,
    apply_event,
)


# ------------------------------------------------------------------ #
# Valid forward transitions (forward skips are allowed)               #
# ------------------------------------------------------------------ #

VALID_FORWARD = [
    # Normal linear progression
    (CallState.QUEUED,    "RESERVE",  CallState.RESERVED),
    (CallState.RESERVED,  "INITIATE", CallState.INITIATED),
    (CallState.INITIATED, "RING",     CallState.RINGING),
    (CallState.RINGING,   "ANSWER",   CallState.ANSWERED),
    (CallState.ANSWERED,  "CONNECT",  CallState.CONNECTED),
    (CallState.CONNECTED, "COMPLETE", CallState.COMPLETED),
    # Forward skips: QUEUED -> INITIATED (provider event arrives late/early)
    (CallState.QUEUED,    "INITIATE", CallState.INITIATED),
    (CallState.QUEUED,    "RING",     CallState.RINGING),
    (CallState.QUEUED,    "ANSWER",   CallState.ANSWERED),
    (CallState.RESERVED,  "RING",     CallState.RINGING),
    (CallState.RESERVED,  "ANSWER",   CallState.ANSWERED),
    (CallState.INITIATED, "ANSWER",   CallState.ANSWERED),
    (CallState.INITIATED, "CONNECT",  CallState.CONNECTED),
    # Terminal transitions from various states
    (CallState.QUEUED,    "CANCEL",   CallState.CANCELLED),
    (CallState.RESERVED,  "CANCEL",   CallState.CANCELLED),
    (CallState.INITIATED, "FAIL",     CallState.FAILED),
    (CallState.RINGING,   "FAIL",     CallState.FAILED),
    (CallState.ANSWERED,  "ABANDON",  CallState.ABANDONED),
    (CallState.RINGING,   "COMPLETE", CallState.COMPLETED),
]


@pytest.mark.parametrize("current, event, expected", VALID_FORWARD)
def test_valid_forward_transitions(current, event, expected):
    new_state, applied, reason = apply_event(current, event)
    assert applied is True, f"Expected applied=True but got reason: {reason}"
    assert new_state == expected
    assert reason == ""


# ------------------------------------------------------------------ #
# Terminal state absorption: every event is rejected                  #
# ------------------------------------------------------------------ #

ALL_EVENTS = ["RESERVE", "INITIATE", "RING", "ANSWER", "CONNECT",
              "COMPLETE", "FAIL", "CANCEL", "ABANDON", "UNKNOWN"]

@pytest.mark.parametrize("terminal_state", list(TERMINAL_STATES))
@pytest.mark.parametrize("event", ALL_EVENTS)
def test_terminal_state_absorbs_all_events(terminal_state, event):
    new_state, applied, reason = apply_event(terminal_state, event)
    assert applied is False
    assert new_state == terminal_state
    assert "terminal" in reason.lower()


# ------------------------------------------------------------------ #
# Backward / same-rank transitions are rejected                       #
# ------------------------------------------------------------------ #

BACKWARD_TRANSITIONS = [
    # Same-rank (both rank 6 terminals — one terminal cannot move to another)
    (CallState.COMPLETED, "FAIL"),
    (CallState.FAILED,    "COMPLETE"),
    (CallState.CANCELLED, "COMPLETE"),
    # Backward moves
    (CallState.RINGING,   "RESERVE"),   # rank 3 -> rank 1
    (CallState.ANSWERED,  "RING"),      # rank 4 -> rank 3
    (CallState.CONNECTED, "ANSWER"),    # rank 5 -> rank 4
    (CallState.CONNECTED, "INITIATE"),  # rank 5 -> rank 2
    (CallState.RINGING,   "INITIATE"),  # rank 3 -> rank 2
]


@pytest.mark.parametrize("current, event", BACKWARD_TRANSITIONS)
def test_backward_transition_rejected(current, event):
    new_state, applied, reason = apply_event(current, event)
    assert applied is False
    assert new_state == current


# ------------------------------------------------------------------ #
# UNKNOWN side-state semantics                                         #
# ------------------------------------------------------------------ #

def test_unknown_from_initiated_is_applied():
    """UNKNOWN is valid from INITIATED — the initiation timed out ambiguously."""
    new_state, applied, reason = apply_event(CallState.INITIATED, "UNKNOWN")
    assert applied is True
    assert new_state == CallState.UNKNOWN
    assert reason == ""


def test_unknown_from_unknown_is_applied():
    """Repeated UNKNOWN events are idempotent."""
    new_state, applied, reason = apply_event(CallState.UNKNOWN, "UNKNOWN")
    assert applied is True
    assert new_state == CallState.UNKNOWN


def test_unknown_from_ringing_is_rejected():
    """Cannot regress to UNKNOWN once we have RINGING — we know the call exists."""
    new_state, applied, reason = apply_event(CallState.RINGING, "UNKNOWN")
    assert applied is False
    assert new_state == CallState.RINGING


def test_ringing_after_unknown_is_applied():
    """
    UNKNOWN is at rank 2 (same as INITIATED).  A RINGING event (rank 3)
    should still advance the state — the call turned out to be real.
    """
    new_state, applied, reason = apply_event(CallState.UNKNOWN, "RING")
    assert applied is True
    assert new_state == CallState.RINGING


def test_fail_after_unknown_is_applied():
    """Sweeper reconciles UNKNOWN -> FAILED if the call never happened."""
    new_state, applied, reason = apply_event(CallState.UNKNOWN, "FAIL")
    assert applied is True
    assert new_state == CallState.FAILED


# ------------------------------------------------------------------ #
# Unknown event names                                                  #
# ------------------------------------------------------------------ #

def test_unknown_event_name_rejected():
    new_state, applied, reason = apply_event(CallState.QUEUED, "TELEPORT")
    assert applied is False
    assert new_state == CallState.QUEUED
    assert "TELEPORT" in reason


def test_event_name_is_case_insensitive():
    new_state, applied, _ = apply_event(CallState.QUEUED, "reserve")
    assert applied is True
    assert new_state == CallState.RESERVED


# ------------------------------------------------------------------ #
# Return value contract                                                #
# ------------------------------------------------------------------ #

def test_apply_event_returns_tuple_of_three():
    result = apply_event(CallState.QUEUED, "RESERVE")
    assert len(result) == 3
    state, applied, reason = result
    assert isinstance(state, CallState)
    assert isinstance(applied, bool)
    assert isinstance(reason, str)


def test_state_rank_ordering():
    """State ranks must be non-decreasing along the normal path."""
    normal_path = [
        CallState.QUEUED, CallState.RESERVED, CallState.INITIATED,
        CallState.RINGING, CallState.ANSWERED, CallState.CONNECTED,
        CallState.COMPLETED,
    ]
    ranks = [STATE_RANK[s] for s in normal_path]
    assert ranks == sorted(ranks)


def test_held_state_transitions():
    """Test transitions into and out of HELD state."""
    # From ANSWERED, HOLD moves to HELD
    state, applied, _ = apply_event(CallState.ANSWERED, "HOLD")
    assert applied is True
    assert state == CallState.HELD

    # Idempotent HOLD on HELD
    state, applied, _ = apply_event(CallState.HELD, "HOLD")
    assert applied is True
    assert state == CallState.HELD

    # From HELD, CONNECT moves to CONNECTED
    state, applied, _ = apply_event(CallState.HELD, "CONNECT")
    assert applied is True
    assert state == CallState.CONNECTED

    # From HELD, ABANDON moves to ABANDONED
    state, applied, _ = apply_event(CallState.HELD, "ABANDON")
    assert applied is True
    assert state == CallState.ABANDONED

    # From ANSWERED, direct ABANDON is valid
    state, applied, _ = apply_event(CallState.ANSWERED, "ABANDON")
    assert applied is True
    assert state == CallState.ABANDONED

    # HOLD from INITIATED is rejected
    state, applied, reason = apply_event(CallState.INITIATED, "HOLD")
    assert applied is False
    assert state == CallState.INITIATED

