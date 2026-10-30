"""
Call finite-state machine — pure functions, no database I/O.

State ranks and the rank-based transition rule
-----------------------------------------------
Each state has an integer rank.  An incoming event is applied only if the
event's target state has a strictly higher rank than the current state.
This enforces two properties simultaneously:
  1. Forward-only (no backward moves): a RINGING event cannot demote an
     ANSWERED call back to RINGING.
  2. Terminal absorption: terminal states (COMPLETED, FAILED, CANCELLED,
     ABANDONED) have the highest ranks.  Nothing can move past them.

Ranks
-----
  0  QUEUED      — Row created, not yet sent to a provider.
  1  RESERVED    — Agent and borrower locked; about to be sent to provider.
  2  INITIATED   — API call to provider returned success (fire-and-forget).
  3  RINGING     — Provider reports phone is ringing at borrower's end.
  4  ANSWERED    — Borrower picked up (human answer, not voicemail).
                   ANSWERED ≠ CONNECTED.  The borrower heard a ring and lifted
                   the handset, but no agent is bridged yet.
  5  CONNECTED   — An agent is actively bridged.  This is the "talk time" state.
  6  COMPLETED   — Call ended normally (agent hung up or borrower hung up after
                   a conversation).   TERMINAL.
  6  FAILED      — Provider could not complete the call (busy, no-answer,
                   network error, etc.).  TERMINAL.  Same rank as COMPLETED
                   because both are valid endpoints; neither can move to the
                   other.
  6  CANCELLED   — Call was cancelled before initiation (e.g. reservation
                   released, campaign paused).  TERMINAL.
  6  ABANDONED   — Borrower answered but no agent was bridged within the
                   configured window.  TERMINAL.  Counts against abandon rate.

UNKNOWN — the side state
------------------------
UNKNOWN has rank 2 (same as INITIATED).  It represents an ambiguous provider
outcome: our initiation request timed out or returned an error that could mean
"call was created" OR "call was not created".  We cannot retry blindly
(principle 6: timeouts are ambiguous) so we keep the call alive with
UNKNOWN state and let the recovery sweeper reconcile via idempotency key.

UNKNOWN is a *side state*, not a rank step: it does NOT increase the rank
above INITIATED.  This means a subsequent valid event (RINGING, ANSWERED, etc.)
can still move the call forward.  Equally, if the sweeper determines the call
never happened, it transitions to FAILED.

Event -> target-state mapping
------------------------------
Each incoming event name maps to the state we *want* to move to.
The apply_event() function validates the rank rule before applying.

  "RESERVE"   -> RESERVED
  "INITIATE"  -> INITIATED
  "RING"      -> RINGING
  "ANSWER"    -> ANSWERED
  "CONNECT"   -> CONNECTED
  "COMPLETE"  -> COMPLETED
  "FAIL"      -> FAILED
  "CANCEL"    -> CANCELLED
  "ABANDON"   -> ABANDONED
  "UNKNOWN"   -> UNKNOWN   (special: sets state without raising rank)
"""

from __future__ import annotations

from enum import Enum


class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    HELD = "HELD"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"
    UNKNOWN = "UNKNOWN"


# Integer rank for each state.
# Terminal states share rank 6 so they absorb each other (first-writer wins
# in the DB, but none can be "promoted" to another terminal state).
# UNKNOWN shares rank 2 with INITIATED so a RINGING event can still advance.
# HELD shares rank 4 with ANSWERED as a sub-state awaiting an available agent.
STATE_RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.UNKNOWN: 2,   # side state — same rank as INITIATED
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.HELD: 4,       # sub-state of ANSWERED awaiting agent bridge
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
    CallState.ABANDONED: 6,
}

# Terminal states absorb everything: once here, no event is applied.
TERMINAL_STATES: frozenset[CallState] = frozenset(
    [CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED, CallState.ABANDONED]
)

# Mapping from raw event name (as received from provider or internal logic)
# to the target CallState.
_EVENT_TO_STATE: dict[str, CallState] = {
    "RESERVE": CallState.RESERVED,
    "INITIATE": CallState.INITIATED,
    "RING": CallState.RINGING,
    "ANSWER": CallState.ANSWERED,
    "HOLD": CallState.HELD,
    "CONNECT": CallState.CONNECTED,
    "COMPLETE": CallState.COMPLETED,
    "FAIL": CallState.FAILED,
    "CANCEL": CallState.CANCELLED,
    "ABANDON": CallState.ABANDONED,
    "UNKNOWN": CallState.UNKNOWN,
}


def apply_event(
    current_state: CallState, event: str
) -> tuple[CallState, bool, str]:
    """
    Attempt to advance a call's state machine given an incoming event.

    Parameters
    ----------
    current_state:
        The call's current state as read from the database.
    event:
        Raw event name (e.g. "RINGING", "ANSWER").  Case-insensitive.

    Returns
    -------
    (new_state, applied, reason)
        new_state : resulting state (unchanged if not applied)
        applied   : True if the transition was applied
        reason    : empty string on success; human-readable rejection reason
                    otherwise.  Always populated when applied=False.

    Rules
    -----
    1. Unknown event name -> rejected with reason.
    2. Current state is terminal -> rejected; terminal states absorb all events.
    3. UNKNOWN event from INITIATED or UNKNOWN -> applied (stays at rank 2).
    4. HOLD event from ANSWERED or HELD -> applied (stays at rank 4).
    5. Target rank <= current rank -> rejected (backward or same-rank move).
    6. Otherwise -> applied.
    """
    event_upper = event.upper()

    # Rule 1: unknown event name.
    if event_upper not in _EVENT_TO_STATE:
        return current_state, False, f"Unknown event '{event}'."

    target_state = _EVENT_TO_STATE[event_upper]

    # Rule 2: terminal absorption.
    if current_state in TERMINAL_STATES:
        return (
            current_state,
            False,
            f"Call is in terminal state {current_state.value}; "
            f"event '{event}' is absorbed.",
        )

    # Rule 3: UNKNOWN is a side state — apply it only from INITIATED or UNKNOWN.
    if target_state is CallState.UNKNOWN:
        if current_state in (CallState.INITIATED, CallState.UNKNOWN):
            return CallState.UNKNOWN, True, ""
        return (
            current_state,
            False,
            f"UNKNOWN event only valid from INITIATED or UNKNOWN; "
            f"current state is {current_state.value}.",
        )

    # Rule 4: HELD is a sub-state of ANSWERED awaiting agent bridge.
    if target_state is CallState.HELD:
        if current_state in (CallState.ANSWERED, CallState.HELD):
            return CallState.HELD, True, ""
        return (
            current_state,
            False,
            f"HOLD event only valid from ANSWERED or HELD; "
            f"current state is {current_state.value}.",
        )

    current_rank = STATE_RANK[current_state]
    target_rank = STATE_RANK[target_state]

    # Rule 4: reject backward or same-rank transitions.
    if target_rank <= current_rank:
        return (
            current_state,
            False,
            f"Backward transition rejected: {current_state.value} (rank {current_rank}) "
            f"-> {target_state.value} (rank {target_rank}).",
        )

    # Rule 5: valid forward transition.
    return target_state, True, ""


def state_rank(state: CallState) -> int:
    """Convenience accessor for STATE_RANK."""
    return STATE_RANK[state]
