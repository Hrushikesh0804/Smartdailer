"""
tests/test_executor.py — Tests for DialExecutor (3-step dial pattern).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.orchestrator.dial_executor import DialExecutor
from smartdialer.providers.base import InitiateResult, InitiateStatus
from smartdialer.providers.mock_a import MockProviderA

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _setup_reserved_call(session_factory):
    s = session_factory()
    try:
        b = Borrower(
            campaign_id=CAMPAIGN_ID,
            phone_token="tok_exec_1",
            state="RESERVED",
            attempts=1,
        )
        s.add(b)
        s.flush()

        a = Agent(
            campaign_id=CAMPAIGN_ID,
            state="RESERVED",
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
        s.commit()
        return c.id, a.id, b.id
    finally:
        s.close()


def test_no_transaction_open_during_provider_call(session_factory):
    """
    Assert that no database transaction is open while calling provider.initiate_call().
    """
    call_id, agent_id, borrower_id = _setup_reserved_call(session_factory)
    clock = FakeClock(NOW)

    class TransactionCheckingProvider(MockProviderA):
        def __init__(self):
            super().__init__(clock=clock)
            self.transaction_was_open = None

        def initiate_call(self, call_id, idempotency_key, to_token):
            # Check DB session
            s = session_factory()
            try:
                # in_transaction() is False when session is freshly opened or not in an active block
                self.transaction_was_open = s.in_transaction()
            finally:
                s.close()
            return super().initiate_call(call_id, idempotency_key, to_token)

    provider = TransactionCheckingProvider()
    executor = DialExecutor(provider=provider, session_factory=session_factory, clock=clock)

    final_state = executor.execute(call_id)
    assert final_state == CallState.INITIATED.value
    assert provider.transaction_was_open is False


def test_executor_accepted_flow(session_factory):
    """
    ACCEPTED -> call=INITIATED, agent=DIALING
    """
    call_id, agent_id, borrower_id = _setup_reserved_call(session_factory)
    clock = FakeClock(NOW)
    provider = MockProviderA(clock=clock, rejection_rate=0.0)
    executor = DialExecutor(provider=provider, session_factory=session_factory, clock=clock)

    state = executor.execute(call_id)
    assert state == CallState.INITIATED.value

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.INITIATED.value
        assert call.provider_call_id is not None
        assert call.version == 1

        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.DIALING.value

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "RESERVED"
    finally:
        s.close()


def test_executor_rejected_flow(session_factory):
    """
    REJECTED -> call=FAILED, agent=AVAILABLE, borrower=ELIGIBLE with backoff
    """
    call_id, agent_id, borrower_id = _setup_reserved_call(session_factory)
    clock = FakeClock(NOW)
    provider = MockProviderA(clock=clock, rejection_rate=1.0)  # force rejection
    executor = DialExecutor(provider=provider, session_factory=session_factory, clock=clock)

    state = executor.execute(call_id)
    assert state == CallState.FAILED.value

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.FAILED.value
        assert call.terminal_reason is not None

        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "ELIGIBLE"
        assert borrower.next_eligible_at > NOW
    finally:
        s.close()


def test_executor_timeout_moves_to_unknown_without_retry(session_factory):
    """
    TIMEOUT -> call=UNKNOWN, reconcile_deadline set, resources remain held.
    NEVER retry on timeout inside the executor.
    """
    call_id, agent_id, borrower_id = _setup_reserved_call(session_factory)
    clock = FakeClock(NOW)

    provider = MockProviderA(clock=clock)
    provider.set_outage(True)  # outage causes TIMEOUT

    executor = DialExecutor(provider=provider, session_factory=session_factory, clock=clock)

    state = executor.execute(call_id)
    assert state == CallState.UNKNOWN.value

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.UNKNOWN.value
        assert call.reconcile_deadline is not None
        assert call.reconcile_deadline > NOW

        # Resources remain held (not released)
        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.RESERVED.value

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "RESERVED"
    finally:
        s.close()
