"""
tests/test_sweeper.py — Tests for the recovery sweeper.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.events.inbox import ingest
from smartdialer.events.processor import process_pending
from smartdialer.orchestrator.dial_executor import DialExecutor
from smartdialer.providers.base import InitiateResult, InitiateStatus, ProviderEvent, ProviderEventType
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.providers.registry import register, clear as clear_registry
from smartdialer.recovery.sweeper import (
    Sweeper,
    get_late_answer_after_cancel_count,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _setup_call_and_agent(session_factory, call_state=CallState.RESERVED, lease_expired=False):
    s = session_factory()
    try:
        b = Borrower(
            campaign_id=CAMPAIGN_ID,
            phone_token="tok_swp",
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

        lease_expires_at = (NOW - timedelta(seconds=10)) if lease_expired else (NOW + timedelta(seconds=120))

        c = Call(
            id=uuid.uuid4(),
            campaign_id=CAMPAIGN_ID,
            borrower_id=b.id,
            agent_id=a.id,
            provider="mock_a",
            state=call_state.value,
            state_rank=STATE_RANK[call_state],
            version=0,
            lease_expires_at=lease_expires_at,
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


def test_two_sweepers_advisory_lock(session_factory):
    """
    Two sweepers started together -> only one acquires the advisory lock and does work.
    The second exits cleanly without raising an error.
    """
    clock = FakeClock(NOW)
    s1 = Sweeper(session_factory, clock=clock)
    s2 = Sweeper(session_factory, clock=clock)

    try:
        assert s1.try_acquire_lock() is True
        # Second instance cannot acquire the same advisory lock
        assert s2.try_acquire_lock() is False
    finally:
        s1.release_lock()
        s2.release_lock()


def test_expired_reserved_lease_worker_crashed_before_dial(session_factory):
    """
    Worker crashed before dialing: lease expires -> call CANCELLED, agent/borrower released.
    """
    call_id, agent_id, borrower_id = _setup_call_and_agent(
        session_factory, call_state=CallState.RESERVED, lease_expired=True
    )
    clock = FakeClock(NOW)
    sweeper = Sweeper(session_factory, clock=clock)

    sweeper.sweep_once()

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.CANCELLED.value
        assert "lease expired before dial" in call.terminal_reason

        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value
        assert agent.reserved_call_id is None

        borrower = s.get(Borrower, borrower_id)
        assert borrower.state == "ELIGIBLE"
    finally:
        s.close()


def test_crash_after_provider_accept_before_txn2_reconciled(session_factory):
    """
    Crash after provider ACCEPT, before Txn 2 (kill the executor via exception) ->
    sweeper reconciles (A via status, B via idempotent re-send) with no second provider call created.
    """
    clock = FakeClock(NOW)

    # 1. Test with Provider A
    call_id_a, agent_id_a, borrower_id_a = _setup_call_and_agent(
        session_factory, call_state=CallState.RESERVED, lease_expired=False
    )
    prov_a = MockProviderA(clock=clock, rejection_rate=0.0)
    register(prov_a)

    class CrashingExecutor(DialExecutor):
        def _apply_accepted(self, session, call, result, now):
            # Simulate crash before committing Txn 2
            raise RuntimeError("Crash before Txn 2 commit")

    exec_a = CrashingExecutor(provider=prov_a, session_factory=session_factory, clock=clock)
    with pytest.raises(RuntimeError, match="Crash before Txn 2 commit"):
        exec_a.execute(call_id_a)

    # Call is still in RESERVED in DB, but provider accepted it!
    # Advance clock past lease
    clock.advance(seconds=130)

    sweeper = Sweeper(session_factory, clock=clock)
    sweeper.sweep_once()

    s = session_factory()
    try:
        call = s.get(Call, call_id_a)
        assert call.state == CallState.INITIATED.value
        assert call.provider_call_id is not None
        agent = s.get(Agent, agent_id_a)
        assert agent.state == AgentState.DIALING.value
    finally:
        s.close()

    # 2. Test with Provider B
    call_id_b, agent_id_b, borrower_id_b = _setup_call_and_agent(
        session_factory, call_state=CallState.RESERVED, lease_expired=False
    )
    s = session_factory()
    try:
        cb = s.get(Call, call_id_b)
        cb.provider = "mock_b"
        s.commit()
    finally:
        s.close()

    prov_b = MockProviderB(clock=clock, timeout_rate=0.0, rejection_rate=0.0)
    register(prov_b)

    exec_b = CrashingExecutor(provider=prov_b, session_factory=session_factory, clock=clock)
    with pytest.raises(RuntimeError, match="Crash before Txn 2 commit"):
        exec_b.execute(call_id_b)

    initial_b_calls = len(prov_b._idempotency_map)

    # Advance clock past lease
    clock.advance(seconds=130)
    sweeper.sweep_once()

    s = session_factory()
    try:
        call = s.get(Call, call_id_b)
        assert call.state == CallState.INITIATED.value
        # No second call created in Provider B
        assert len(prov_b._idempotency_map) == initial_b_calls
    finally:
        s.close()


def test_provider_b_timeout_both_hidden_outcomes(session_factory):
    """
    ProviderB TIMEOUT, both hidden outcomes (accepted / not accepted) end consistent.
    """
    clock = FakeClock(NOW)

    # Case 1: Provider B secretly accepted
    call_id_1, a_id_1, b_id_1 = _setup_call_and_agent(session_factory, call_state=CallState.RESERVED)
    prov_b_accepted = MockProviderB(
        clock=clock, timeout_rate=1.0, timeout_secretly_accepted_rate=1.0, rejection_rate=0.0
    )
    register(prov_b_accepted)

    s = session_factory()
    try:
        c1 = s.get(Call, call_id_1)
        c1.provider = "mock_b"
        s.commit()
    finally:
        s.close()

    exec_1 = DialExecutor(provider=prov_b_accepted, session_factory=session_factory, clock=clock)
    state_1 = exec_1.execute(call_id_1)
    assert state_1 == CallState.UNKNOWN.value

    # Sweeper reconciles
    sweeper = Sweeper(session_factory, clock=clock)
    sweeper.sweep_once()

    s = session_factory()
    try:
        c1 = s.get(Call, call_id_1)
        # Re-send on B returned the secretly accepted call -> INITIATED
        assert c1.state == CallState.INITIATED.value
        a1 = s.get(Agent, a_id_1)
        assert a1.state == AgentState.DIALING.value
    finally:
        s.close()

    # Case 2: Provider B did NOT secretly accept
    call_id_2, a_id_2, b_id_2 = _setup_call_and_agent(session_factory, call_state=CallState.RESERVED)
    prov_b_not_accepted = MockProviderB(
        clock=clock, timeout_rate=1.0, timeout_secretly_accepted_rate=0.0, rejection_rate=0.0
    )

    s = session_factory()
    try:
        c2 = s.get(Call, call_id_2)
        c2.provider = "mock_b"
        s.commit()
    finally:
        s.close()

    exec_2 = DialExecutor(provider=prov_b_not_accepted, session_factory=session_factory, clock=clock)
    state_2 = exec_2.execute(call_id_2)
    assert state_2 == CallState.UNKNOWN.value

    # During sweep, if provider B keeps timing out past deadline, call moves to FAILED/CANCELLED
    s = session_factory()
    try:
        c2 = s.get(Call, call_id_2)
        c2.reconcile_deadline = NOW - timedelta(seconds=1)
        s.commit()
    finally:
        s.close()

    sweeper.sweep_once()

    s = session_factory()
    try:
        c2 = s.get(Call, call_id_2)
        assert c2.state in (CallState.FAILED.value, CallState.CANCELLED.value)
        a2 = s.get(Agent, a_id_2)
        assert a2.state == AgentState.AVAILABLE.value
    finally:
        s.close()


def test_cancel_while_answered_event_in_flight(session_factory):
    """
    Cancel while an ANSWERED event is in flight -> consistent terminal state, agent released exactly once.
    Late ANSWERED arrives -> stored IGNORED_LATE, emits late_answer_after_cancel log.
    """
    call_id, agent_id, borrower_id = _setup_call_and_agent(session_factory, call_state=CallState.INITIATED)
    clock = FakeClock(NOW)

    prov_call_id = "prov_flight_123"
    s = session_factory()
    try:
        call = s.get(Call, call_id)
        call.provider_call_id = prov_call_id
        s.commit()
    finally:
        s.close()

    # Cancel the call via sweeper release
    s = session_factory()
    try:
        sweeper = Sweeper(session_factory, clock=clock)
        call = s.get(Call, call_id)
        sweeper._release_call(s, call, reason="user_cancelled", terminal_state=CallState.CANCELLED, now=NOW)
        s.commit()
    finally:
        s.close()

    # Verify agent was released to AVAILABLE
    s = session_factory()
    try:
        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value
        call = s.get(Call, call_id)
        assert call.state == CallState.CANCELLED.value
    finally:
        s.close()

    initial_late_count = get_late_answer_after_cancel_count()

    # Now in-flight ANSWERED arrives
    s = session_factory()
    try:
        evt = ProviderEvent(
            event_id="ev_in_flight_ans",
            provider_call_id=prov_call_id,
            event_type=ProviderEventType.ANSWERED,
        )
        ingest(s, "mock_a", evt, clock=clock)
    finally:
        s.close()

    process_pending(session_factory, batch_size=10, clock=clock)

    # Verify event stored as IGNORED_LATE
    s = session_factory()
    try:
        ev = s.scalars(select(CallEvent).where(CallEvent.event_id == "ev_in_flight_ans")).one()
        assert ev.status == "IGNORED_LATE"

        # Call remains CANCELLED (terminal absorbed)
        call = s.get(Call, call_id)
        assert call.state == CallState.CANCELLED.value

        # Agent remains AVAILABLE (not re-locked or changed)
        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value

        # Counter was incremented
        assert get_late_answer_after_cancel_count() == initial_late_count + 1
    finally:
        s.close()


def test_stale_agent_released(session_factory):
    """
    Stale agent pointing to a terminal/missing call -> released.
    """
    call_id, agent_id, borrower_id = _setup_call_and_agent(session_factory, call_state=CallState.COMPLETED)
    clock = FakeClock(NOW)

    # Force agent back to RESERVED while call is COMPLETED
    s = session_factory()
    try:
        agent = s.get(Agent, agent_id)
        agent.state = AgentState.RESERVED.value
        agent.reserved_call_id = call_id
        s.commit()
    finally:
        s.close()

    sweeper = Sweeper(session_factory, clock=clock)
    sweeper.sweep_once()

    s = session_factory()
    try:
        agent = s.get(Agent, agent_id)
        assert agent.state == AgentState.AVAILABLE.value
        assert agent.reserved_call_id is None
    finally:
        s.close()
