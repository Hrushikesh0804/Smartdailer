"""
tests/test_orchestrator_tick.py — Integration tests for tick execution and two-phase safety checks.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call, SafetyDecisionLog
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.orchestrator.tick import run_tick
from smartdialer.providers.base import ProviderEvent, ProviderEventType
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.registry import register
from smartdialer.safety.controller import (
    PacingMode,
    PacingRequest,
    SafetyAction,
    SafetyDecision,
    advise,
    build_snapshot,
    enforce,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _populate_agents_and_borrowers(session_factory, n_agents=3, n_borrowers=5):
    s = session_factory()
    try:
        agents = []
        for i in range(n_agents):
            a = Agent(
                campaign_id=CAMPAIGN_ID,
                state="AVAILABLE",
                last_state_change_at=NOW,
                updated_at=NOW,
            )
            s.add(a)
            agents.append(a)

        borrowers = []
        for i in range(n_borrowers):
            b = Borrower(
                campaign_id=CAMPAIGN_ID,
                phone_token=f"tok_tick_{i}",
                state="ELIGIBLE",
                priority=10 - i,
            )
            s.add(b)
            borrowers.append(b)

        s.commit()
        return [a.id for a in agents], [b.id for b in borrowers]
    finally:
        s.close()


def test_enforce_reduces_advisory_approval_when_state_changed(session_factory):
    """
    Two-phase check:
    Pacing/advise saw 3 available agents and approved 3.
    Before allocator transaction commits, 2 agents went OFFLINE.
    safety.enforce must detect fresh DB count and reduce approval to 1.
    """
    agent_ids, _ = _populate_agents_and_borrowers(session_factory, n_agents=3, n_borrowers=5)
    clock = FakeClock(NOW)

    # Phase 1: Snapshot and Advisory Check
    s_read = session_factory()
    try:
        snap = build_snapshot(s_read, campaign_id=CAMPAIGN_ID, now=NOW)
    finally:
        s_read.close()

    assert snap.free_agents == 3
    req = PacingRequest(campaign_id=CAMPAIGN_ID, mode=PacingMode.PROGRESSIVE, requested_calls=3)
    advisory_dec = advise(snap, req)
    assert advisory_dec.action == SafetyAction.APPROVE
    assert advisory_dec.approved_calls == 3

    # Intermediate state change: 2 agents log out before allocator commits
    s_mod = session_factory()
    try:
        for aid in agent_ids[:2]:
            a = s_mod.get(Agent, aid)
            a.state = AgentState.OFFLINE.value
        s_mod.commit()
    finally:
        s_mod.close()

    # Phase 2: In-transaction enforce
    s_enforce = session_factory()
    try:
        enforce_dec = enforce(
            s_enforce,
            request=req,
            now=NOW,
            advisory_limit=advisory_dec.approved_calls,
        )
        s_enforce.commit()
    finally:
        s_enforce.close()

    # Enforce re-validated against fresh count (1) and reduced approved calls
    assert enforce_dec.approved_calls == 1
    assert enforce_dec.action == SafetyAction.REDUCE

    # Assert decision was logged to safety_decisions table
    s_verify = session_factory()
    try:
        logs = s_verify.scalars(select(SafetyDecisionLog).order_by(SafetyDecisionLog.id.desc())).all()
        assert len(logs) >= 1
        latest = logs[0]
        assert latest.approved_calls == 1
        assert latest.action == "REDUCE"
    finally:
        s_verify.close()


def test_agent_set_offline_between_pacing_and_allocation(session_factory):
    """
    When all agents go OFFLINE between pacing and allocation,
    no call is placed and nothing is leaked in RESERVED state.
    """
    agent_ids, _ = _populate_agents_and_borrowers(session_factory, n_agents=2, n_borrowers=5)
    clock = FakeClock(NOW)

    # All agents log out
    s = session_factory()
    try:
        for aid in agent_ids:
            a = s.get(Agent, aid)
            a.state = AgentState.OFFLINE.value
        s.commit()
    finally:
        s.close()

    summary = run_tick(
        session_factory=session_factory,
        campaign_id=CAMPAIGN_ID,
        provider_name="mock_a",
        clock=clock,
    )

    assert summary["allocated_calls"] == 0
    assert summary["dialed_calls"] == 0

    s = session_factory()
    try:
        reserved_agents = s.scalars(select(Agent).where(Agent.state == "RESERVED")).all()
        assert len(reserved_agents) == 0
        calls = s.scalars(select(Call)).all()
        assert len(calls) == 0
    finally:
        s.close()


def test_provider_outage_trips_circuit_and_blocks_new_dials(session_factory):
    """
    Provider outage -> circuit OPEN:
    - Safety blocks any new dials (approved=0).
    - Existing in-flight calls and pending events are still processed.
    """
    _populate_agents_and_borrowers(session_factory, n_agents=3, n_borrowers=5)
    clock = FakeClock(NOW)

    provider = MockProviderA(clock=clock)
    register(provider)

    # Force provider circuit breaker OPEN
    from smartdialer.providers.health import CircuitBreaker, CircuitState
    from smartdialer.config import settings

    breaker = CircuitBreaker("mock_a", clock=clock)
    for _ in range(settings.circuit_breaker_min_calls):
        breaker.record(success=False, latency_ms=1000.0)
    assert breaker.state == CircuitState.OPEN

    # Attach health snapshot to mock provider so build_snapshot sees it
    provider.health_snapshot = breaker.snapshot

    # Run tick: safety R4 must reject and block all dials
    summary = run_tick(
        session_factory=session_factory,
        campaign_id=CAMPAIGN_ID,
        provider_name="mock_a",
        clock=clock,
    )

    assert summary["advisory_action"] == SafetyAction.REJECT.value
    assert summary["approved_calls"] == 0
    assert summary["allocated_calls"] == 0
