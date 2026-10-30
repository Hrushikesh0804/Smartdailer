"""
smartdialer.pacing.progressive — Progressive dialer pacing and lifecycle handlers.

Contract & Responsibilities
---------------------------
1. Propose dial count: in progressive mode, requested = free_agents.
2. Failed call handling:
   - Release agent immediately back to AVAILABLE.
   - Borrower attempts + 1 with exponential retry backoff.
   - Cap on attempts -> terminal state (DONE or DO_NOT_CALL).
3. Agent lost during setup:
   - If an agent goes OFFLINE / PAUSED while RESERVED / DIALING:
   - Cancel call (terminal_reason = "agent_lost").
   - Release borrower with NO attempt penalty (attempts not incremented, eligible immediately).
   - Agent is NOT re-pooled (remains in OFFLINE / PAUSED).

Note on Architecture Boundary
-----------------------------
This module does NOT import or reference system_flags, kill-switch, or any
safety mutators. Safety Controller is independent and authoritative.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.safety.controller import PacingMode, PacingRequest, StateSnapshot

logger = logging.getLogger(__name__)


def calculate_request(snapshot: StateSnapshot) -> PacingRequest:
    """
    Calculate progressive dial proposal.
    Requested calls exactly equals currently available (free) agents.
    """
    return PacingRequest(
        campaign_id=snapshot.campaign_id,
        mode=PacingMode.PROGRESSIVE,
        requested_calls=max(0, snapshot.free_agents),
    )


def handle_call_failure(
    session: Session,
    call_id: uuid.UUID,
    reason: str,
    now: datetime | None = None,
) -> None:
    """
    Handle a failed outbound call:
    - Release agent immediately to AVAILABLE.
    - Increment borrower attempts.
    - If attempts >= max_borrower_attempts: move to terminal state (DONE/DO_NOT_CALL).
    - Otherwise schedule exponential backoff cool-down.
    - Set call state to FAILED.
    """
    _now = now or SystemClock().now()
    call = session.get(Call, call_id, with_for_update=True)
    if call is None:
        return

    # Update call state
    call.state = CallState.FAILED.value
    call.state_rank = STATE_RANK[CallState.FAILED]
    call.terminal_reason = reason
    call.updated_at = _now
    call.version += 1

    # Release agent immediately
    if call.agent_id:
        agent = session.get(Agent, call.agent_id, with_for_update=True)
        if agent and agent.state in (AgentState.RESERVED.value, AgentState.DIALING.value):
            agent.state = AgentState.AVAILABLE.value
            agent.reserved_call_id = None
            agent.reservation_expires_at = None
            agent.last_state_change_at = _now
            agent.updated_at = _now
            agent.version += 1

    # Update borrower with exponential backoff & max attempt cap
    borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
    if borrower:
        borrower.attempts += 1
        if borrower.attempts >= settings.max_borrower_attempts:
            borrower.state = settings.borrower_terminal_state_on_max_attempts
            borrower.next_eligible_at = None
        else:
            borrower.state = "ELIGIBLE"
            delay = min(
                settings.borrower_base_backoff_seconds * (2 ** (borrower.attempts - 1)),
                settings.borrower_max_backoff_seconds,
            )
            borrower.next_eligible_at = _now + timedelta(seconds=delay)

    session.flush()


def handle_agent_lost(
    session: Session,
    call_id: uuid.UUID,
    now: datetime | None = None,
) -> None:
    """
    Handle scenario where agent disappears during call setup (goes OFFLINE/PAUSED):
    - Cancel call with reason "agent_lost".
    - Release borrower with NO attempt penalty (attempts not incremented).
    - Agent is not re-pooled (left in OFFLINE/PAUSED).
    """
    _now = now or SystemClock().now()
    call = session.get(Call, call_id, with_for_update=True)
    if call is None:
        return

    call.state = CallState.CANCELLED.value
    call.state_rank = STATE_RANK[CallState.CANCELLED]
    call.terminal_reason = "agent_lost"
    call.updated_at = _now
    call.version += 1

    # Clear reservation reference on agent without changing agent's current state (e.g. OFFLINE/PAUSED)
    if call.agent_id:
        agent = session.get(Agent, call.agent_id, with_for_update=True)
        if agent:
            agent.reserved_call_id = None
            agent.reservation_expires_at = None
            agent.updated_at = _now

    # Return borrower to ELIGIBLE with zero penalty
    borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
    if borrower and borrower.state == "RESERVED":
        borrower.state = "ELIGIBLE"
        borrower.next_eligible_at = _now  # immediately eligible

    session.flush()
