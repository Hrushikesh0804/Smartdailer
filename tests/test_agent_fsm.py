"""
tests/test_agent_fsm.py — Unit tests for the agent finite-state machine.

Tests are pure (no DB).  Every listed transition in agent_fsm.py is covered,
plus all explicitly invalid transitions that matter for safety.
"""

import pytest
from smartdialer.domain.agent_fsm import AgentState, apply_transition, can_transition


# ------------------------------------------------------------------ #
# Valid transitions                                                    #
# ------------------------------------------------------------------ #

VALID_TRANSITIONS = [
    (AgentState.OFFLINE, AgentState.AVAILABLE),
    (AgentState.AVAILABLE, AgentState.OFFLINE),
    (AgentState.AVAILABLE, AgentState.RESERVED),
    (AgentState.AVAILABLE, AgentState.PAUSED),
    (AgentState.RESERVED, AgentState.DIALING),
    (AgentState.RESERVED, AgentState.AVAILABLE),
    (AgentState.RESERVED, AgentState.OFFLINE),
    (AgentState.RESERVED, AgentState.PAUSED),
    (AgentState.DIALING, AgentState.CONNECTED),
    (AgentState.DIALING, AgentState.AVAILABLE),
    (AgentState.DIALING, AgentState.OFFLINE),
    (AgentState.DIALING, AgentState.PAUSED),
    (AgentState.CONNECTED, AgentState.WRAP_UP),
    (AgentState.CONNECTED, AgentState.OFFLINE),
    (AgentState.WRAP_UP, AgentState.AVAILABLE),
    (AgentState.WRAP_UP, AgentState.OFFLINE),
    (AgentState.WRAP_UP, AgentState.PAUSED),
    (AgentState.PAUSED, AgentState.AVAILABLE),
    (AgentState.PAUSED, AgentState.OFFLINE),
]


@pytest.mark.parametrize("from_state, to_state", VALID_TRANSITIONS)
def test_valid_transition_allowed(from_state, to_state):
    assert can_transition(from_state, to_state) is True


@pytest.mark.parametrize("from_state, to_state", VALID_TRANSITIONS)
def test_apply_valid_transition(from_state, to_state):
    new_state, applied, reason = apply_transition(from_state, to_state)
    assert applied is True
    assert new_state == to_state
    assert reason == ""


# ------------------------------------------------------------------ #
# Invalid transitions                                                  #
# ------------------------------------------------------------------ #

INVALID_TRANSITIONS = [
    # Self-transitions
    (AgentState.AVAILABLE, AgentState.AVAILABLE),
    (AgentState.OFFLINE, AgentState.OFFLINE),
    (AgentState.CONNECTED, AgentState.CONNECTED),
    # Skip a step (OFFLINE -> RESERVED skips AVAILABLE)
    (AgentState.OFFLINE, AgentState.RESERVED),
    (AgentState.OFFLINE, AgentState.DIALING),
    (AgentState.OFFLINE, AgentState.CONNECTED),
    # Backward moves
    (AgentState.CONNECTED, AgentState.RESERVED),
    (AgentState.CONNECTED, AgentState.DIALING),
    (AgentState.DIALING, AgentState.RESERVED),
    (AgentState.WRAP_UP, AgentState.CONNECTED),
    (AgentState.WRAP_UP, AgentState.DIALING),
    (AgentState.WRAP_UP, AgentState.RESERVED),
    # CONNECTED -> PAUSED is NOT allowed: call continues, agent leaves via WRAP_UP
    (AgentState.CONNECTED, AgentState.PAUSED),
    # PAUSED -> RESERVED is NOT allowed: must go PAUSED -> AVAILABLE -> RESERVED
    (AgentState.PAUSED, AgentState.RESERVED),
    (AgentState.PAUSED, AgentState.CONNECTED),
]


@pytest.mark.parametrize("from_state, to_state", INVALID_TRANSITIONS)
def test_invalid_transition_rejected(from_state, to_state):
    assert can_transition(from_state, to_state) is False


@pytest.mark.parametrize("from_state, to_state", INVALID_TRANSITIONS)
def test_apply_invalid_transition(from_state, to_state):
    new_state, applied, reason = apply_transition(from_state, to_state)
    assert applied is False
    assert new_state == from_state  # state unchanged
    assert reason != ""             # reason must be non-empty


# ------------------------------------------------------------------ #
# Specific semantic checks                                             #
# ------------------------------------------------------------------ #

def test_connected_to_offline_is_allowed():
    """
    Supervisor can force-logout an agent who is on a live call.
    The call continues headless; the agent FSM just moves to OFFLINE.
    """
    _, applied, _ = apply_transition(AgentState.CONNECTED, AgentState.OFFLINE)
    assert applied is True


def test_connected_to_paused_is_not_allowed():
    """
    An agent cannot pause while connected — the call is live.
    They must wait for WRAP_UP before pausing.
    """
    _, applied, _ = apply_transition(AgentState.CONNECTED, AgentState.PAUSED)
    assert applied is False


def test_reserved_to_available_release_path():
    """
    When a reservation is cancelled (timeout, campaign pause), the agent
    goes RESERVED -> AVAILABLE.  This must be valid.
    """
    _, applied, _ = apply_transition(AgentState.RESERVED, AgentState.AVAILABLE)
    assert applied is True


def test_reason_is_populated_on_rejection():
    new_state, applied, reason = apply_transition(
        AgentState.OFFLINE, AgentState.RESERVED
    )
    assert not applied
    assert "OFFLINE" in reason
    assert "RESERVED" in reason
