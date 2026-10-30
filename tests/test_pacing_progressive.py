"""
tests/test_pacing_progressive.py — Tests for progressive pacing and lifecycle handlers.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.pacing.progressive import (
    calculate_request,
    handle_agent_lost,
    handle_call_failure,
)
from smartdialer.safety.controller import PacingMode, StateSnapshot

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _create_call_setup(session_factory, attempts=0):
    s = session_factory()
    try:
        b = Borrower(
            campaign_id=CAMPAIGN_ID,
            phone_token="tok_pac_1",
            state="RESERVED",
            attempts=attempts,
        )
        s.add(b)
        s.flush()

        a = Agent(
            campaign_id=CAMPAIGN_ID,
            state=AgentState.RESERVED.value,
            last_state_change_at=NOW,
            updated_at=NOW,
        )
        s.add(a)
        s.flush()

        c = Call(
            id=uuid.uuid4(),
            campaign_id=CAMPAIGN_ID,
            borrower_id=b.id,
            agent_id=a.id,
            provider="mock_a",
            state=CallState.RESERVED.value,
            state_rank=STATE_RANK[CallState.RESERVED],
            version=0,
            created_at=NOW,
            updated_at=NOW,
        )
        s.add(c)
        s.flush()

        a.reserved_call_id = c.id
        s.commit()
        return c.id, a.id, b.id
    finally:
        s.close()


def test_progressive_proposal_matches_free_agents():
    """calculate_request requests exactly free_agents."""
    snap = StateSnapshot(campaign_id=CAMPAIGN_ID, free_agents=7)
    req = calculate_request(snap)
    assert req.mode == PacingMode.PROGRESSIVE
    assert req.requested_calls == 7


def test_failed_call_releases_agent_and_schedules_borrower_backoff(session_factory):
    """
    Failed call releases agent to AVAILABLE immediately, increments borrower attempts,
    and sets next_eligible_at with exponential backoff.
    """
    call_id, agent_id, borrower_id = _create_call_setup(session_factory, attempts=0)

    s = session_factory()
    try:
        handle_call_failure(s, call_id=call_id, reason="BUSY", now=NOW)
        s.commit()
    finally:
        s.close()

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.FAILED.value
        assert call.terminal_reason == "BUSY"

        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value
        assert agent.reserved_call_id is None

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "ELIGIBLE"
        assert borrower.attempts == 1
        # Delay for attempt 1: base * 2^(1-1) = base (default 300s)
        expected_delay = settings.borrower_base_backoff_seconds
        assert (borrower.next_eligible_at - NOW).total_seconds() == expected_delay
    finally:
        s.close()


def test_failed_call_max_attempts_moves_to_done(session_factory):
    """
    When borrower reaches max attempts, moved to DONE / DO_NOT_CALL per config.
    """
    # Start at max_attempts - 1
    call_id, agent_id, borrower_id = _create_call_setup(
        session_factory, attempts=settings.max_borrower_attempts - 1
    )

    s = session_factory()
    try:
        handle_call_failure(s, call_id=call_id, reason="NO_ANSWER", now=NOW)
        s.commit()
    finally:
        s.close()

    s = session_factory()
    try:
        borrower = s.get(Borrower, borrower_id)
        assert borrower.attempts == settings.max_borrower_attempts
        assert borrower.state == settings.borrower_terminal_state_on_max_attempts
        assert borrower.next_eligible_at is None
    finally:
        s.close()


def test_agent_lost_during_setup_no_borrower_penalty(session_factory):
    """
    Agent lost during setup (OFFLINE/PAUSED):
    - Call cancelled with terminal_reason 'agent_lost'.
    - Borrower returned to ELIGIBLE with zero penalty (attempts NOT incremented).
    - Agent left in its state (not re-pooled).
    """
    call_id, agent_id, borrower_id = _create_call_setup(session_factory, attempts=2)

    # Simulate agent disconnecting mid-setup
    s = session_factory()
    try:
        agent = s.get(Agent, agent_id)
        agent.state = AgentState.OFFLINE.value
        s.commit()
    finally:
        s.close()

    s = session_factory()
    try:
        handle_agent_lost(s, call_id=call_id, now=NOW)
        s.commit()
    finally:
        s.close()

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.CANCELLED.value
        assert call.terminal_reason == "agent_lost"

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "ELIGIBLE"
        # Attempts must NOT have increased!
        assert borrower.attempts == 2
        # Must be eligible immediately
        assert borrower.next_eligible_at == NOW

        agent = s.get(Agent, agent_id)
        # Agent remains OFFLINE (not re-pooled into AVAILABLE)
        assert agent.state == AgentState.OFFLINE.value
        assert agent.reserved_call_id is None
    finally:
        s.close()
