"""
Agent finite-state machine — pure functions, no database I/O.

States
------
OFFLINE   : Agent is logged out or disconnected.
AVAILABLE : Agent is logged in and waiting for a call.
RESERVED  : Allocator has earmarked this agent for an imminent call;
            reservation expires after settings.agent_reservation_ttl_seconds.
DIALING   : Provider has been asked to initiate the call; waiting for RINGING.
CONNECTED : Agent's phone is bridged to the borrower (call is live).
WRAP_UP   : Call has ended; agent is completing post-call work.
PAUSED    : Agent has temporarily stepped away (break, supervisor hold, etc.).

Allowed transitions (explicit allowlist — anything not listed is rejected)
---------------------------------------------------------------------------
OFFLINE    -> AVAILABLE          (agent logs in)
AVAILABLE  -> RESERVED           (allocator picks this agent)
AVAILABLE  -> OFFLINE            (agent logs out while idle)
AVAILABLE  -> PAUSED             (agent takes a break)
RESERVED   -> DIALING            (call successfully initiated with provider)
RESERVED   -> AVAILABLE          (reservation released: timeout / cancel)
RESERVED   -> OFFLINE            (agent force-logged-out during reservation)
RESERVED   -> PAUSED             (agent paused during reservation — see note)
DIALING    -> CONNECTED          (provider reports human answered)
DIALING    -> AVAILABLE          (provider reports failure before answer)
DIALING    -> OFFLINE            (agent disconnected mid-dial)
DIALING    -> PAUSED             (agent paused mid-dial — see note)
CONNECTED  -> WRAP_UP            (call ends; agent enters wrap-up)
CONNECTED  -> OFFLINE            (agent disconnected; call continues headless)
WRAP_UP    -> AVAILABLE          (wrap-up complete)
WRAP_UP    -> OFFLINE            (agent logs out during wrap-up)
WRAP_UP    -> PAUSED             (agent pauses during wrap-up)
PAUSED     -> AVAILABLE          (agent returns from break)
PAUSED     -> OFFLINE            (agent logs out while paused)

Notes on RESERVED/DIALING -> PAUSED and -> OFFLINE
----------------------------------------------------
When an agent goes OFFLINE or PAUSED while RESERVED:
  • The agent state is updated.
  • The recovery sweeper (Phase 6) detects that the reservation has no valid
    AVAILABLE agent and cancels the corresponding call row (RESERVED -> CANCELLED).
  • The borrower is returned to ELIGIBLE.
  This is explicit: the FSM here only validates the agent transition; the
  orchestrator/sweeper handles the consequent call-side effect.

When an agent goes OFFLINE while DIALING:
  • The dial attempt continues in the provider — we cannot cancel mid-air.
  • The recovery sweeper waits for a provider event (ANSWERED / FAILED) or
    for the call lease to expire, then reconciles.

When an agent goes OFFLINE or PAUSED while CONNECTED:
  • The provider's bridge is already live — the call continues.
  • The agent's side is dropped; the call is marked ABANDONED after
    settings.call_lease_ttl_seconds if no new agent bridges in.
  • CONNECTED -> OFFLINE is explicitly allowed so supervisors can force-logout
    without leaving the FSM in an invalid state.
"""

from __future__ import annotations

from enum import Enum


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


# Explicit allowlist of (from_state, to_state) pairs.
# Any pair not in this set is an invalid transition.
_ALLOWED_TRANSITIONS: frozenset[tuple[AgentState, AgentState]] = frozenset(
    [
        # Login / logout
        (AgentState.OFFLINE, AgentState.AVAILABLE),
        (AgentState.AVAILABLE, AgentState.OFFLINE),
        (AgentState.RESERVED, AgentState.OFFLINE),
        (AgentState.DIALING, AgentState.OFFLINE),
        (AgentState.CONNECTED, AgentState.OFFLINE),
        (AgentState.WRAP_UP, AgentState.OFFLINE),
        (AgentState.PAUSED, AgentState.OFFLINE),
        # Normal call flow
        (AgentState.AVAILABLE, AgentState.RESERVED),
        (AgentState.RESERVED, AgentState.DIALING),
        (AgentState.DIALING, AgentState.CONNECTED),
        (AgentState.CONNECTED, AgentState.WRAP_UP),
        (AgentState.WRAP_UP, AgentState.AVAILABLE),
        # Release / failure paths
        (AgentState.RESERVED, AgentState.AVAILABLE),  # reservation timed out / cancelled
        (AgentState.DIALING, AgentState.AVAILABLE),   # provider reported failure
        # Pause / resume
        (AgentState.AVAILABLE, AgentState.PAUSED),
        (AgentState.RESERVED, AgentState.PAUSED),
        (AgentState.DIALING, AgentState.PAUSED),
        (AgentState.WRAP_UP, AgentState.PAUSED),
        (AgentState.PAUSED, AgentState.AVAILABLE),
        (AgentState.PAUSED, AgentState.OFFLINE),
    ]
)


def can_transition(from_state: AgentState, to_state: AgentState) -> bool:
    """
    Return True if the agent transition from_state -> to_state is allowed.

    This is an allowlist check: only explicitly listed pairs return True.
    Self-transitions (same state -> same state) return False because they
    indicate a logic error in the caller, not a valid no-op.
    """
    return (from_state, to_state) in _ALLOWED_TRANSITIONS


def apply_transition(
    from_state: AgentState, to_state: AgentState
) -> tuple[AgentState, bool, str]:
    """
    Attempt to transition an agent from from_state to to_state.

    Returns
    -------
    (new_state, applied, reason)
        new_state : the resulting state (unchanged if not applied)
        applied   : True if the transition was applied
        reason    : empty string on success; human-readable rejection reason otherwise
    """
    if can_transition(from_state, to_state):
        return to_state, True, ""
    return (
        from_state,
        False,
        f"Transition {from_state.value} -> {to_state.value} is not allowed.",
    )
