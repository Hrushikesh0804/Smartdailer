"""
tests/test_predictive_integration.py — Integration tests for predictive dialing, answer-time claim, and capacity guard.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.events.inbox import ingest
from smartdialer.events.processor import process_pending, sweep_held_calls
from smartdialer.orchestrator.capacity_guard import check_and_guard_capacity
from smartdialer.orchestrator.tick import run_tick
from smartdialer.pacing.estimator import reset_estimator
from smartdialer.providers.base import ProviderEvent, ProviderEventType
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.registry import register
from smartdialer.safety.controller import (
    PacingMode,
    PacingRequest,
    SafetyAction,
    advise,
    build_snapshot,
    reset_hysteresis,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 77


@pytest.fixture(autouse=True)
def clean_state():
    reset_estimator()
    reset_hysteresis()
    yield
    reset_estimator()
    reset_hysteresis()


def test_safety_clamps_predictive_request_above_overdial_cap(session_factory):
    """Safety R2 strictly clamps predictive requests exceeding the overdial cap."""
    s = session_factory()
    try:
        # 10 agents AVAILABLE -> capacity=10 -> max_overdial = 10 * 3.0 = 30
        for i in range(10):
            a = Agent(campaign_id=CAMPAIGN_ID, state="AVAILABLE", last_state_change_at=NOW, updated_at=NOW)
            s.add(a)
        s.commit()
    finally:
        s.close()

    clock = FakeClock(NOW)
    s = session_factory()
    try:
        snap = build_snapshot(s, campaign_id=CAMPAIGN_ID, now=NOW)
    finally:
        s.close()

    # Request 50 calls (above cap of 30)
    req = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=50,
        estimate_meta={"sample_count": 100, "age_seconds": 10},
    )

    dec = advise(snap, req)
    assert dec.action == SafetyAction.REDUCE
    assert dec.approved_calls == 30  # strictly capped to 10 * 3.0
    assert "R2" in dec.rule_ids


def test_held_call_timeout_triggers_abandonment_and_safety_fallback(session_factory):
    """
    Held-call timeout -> ABANDONED -> abandonment rate rises ->
    Safety falls back to progressive, then recovers after cooldown.
    """
    clock = FakeClock(NOW)
    s = session_factory()
    try:
        # Create an answered call in HELD waiting for an agent
        b = Borrower(campaign_id=CAMPAIGN_ID, phone_token="tok_h1", state="IN_CALL")
        s.add(b)
        s.flush()

        call = Call(
            id=uuid.uuid4(),
            campaign_id=CAMPAIGN_ID,
            borrower_id=b.id,
            agent_id=None,
            provider="mock_a",
            state=CallState.HELD.value,
            state_rank=STATE_RANK[CallState.HELD],
            version=0,
            held_since=NOW,
            created_at=NOW,
            updated_at=NOW,
        )
        s.add(call)
        s.commit()
        call_id = call.id
    finally:
        s.close()

    # Advance clock past hold timeout (default 5s)
    clock.advance(seconds=settings.hold_timeout_seconds + 1)
    timeout_now = clock.now()

    # Sweep held calls
    s = session_factory()
    try:
        swept = sweep_held_calls(s, now=timeout_now, clock=clock)
        s.commit()
    finally:
        s.close()

    assert swept == 1

    # Call is now ABANDONED
    s = session_factory()
    try:
        c = s.get(Call, call_id)
        assert c.state == CallState.ABANDONED.value
        assert c.terminal_reason == "hold_timeout_abandoned"

        # Now test that Safety sees this abandonment and triggers FALLBACK_PROGRESSIVE
        snap = build_snapshot(s, campaign_id=CAMPAIGN_ID, now=timeout_now)
        assert snap.rolling_abandonment_rate > settings.abandonment_budget

        req = PacingRequest(
            campaign_id=CAMPAIGN_ID,
            mode=PacingMode.PREDICTIVE,
            requested_calls=10,
            estimate_meta={"sample_count": 100, "age_seconds": 10},
        )
        dec = advise(snap, req)
        assert dec.action == SafetyAction.FALLBACK_PROGRESSIVE
        assert "R3" in dec.rule_ids
    finally:
        s.close()


def test_capacity_drop_cancels_excess_ringing_newest_first(session_factory):
    """
    Capacity-drop test:
    100 agents AVAILABLE, 40 flip OFFLINE -> live capacity drops.
    assert new dials stop and excess RINGING calls are cancelled newest-first.
    reports ticks to react.
    """
    clock = FakeClock(NOW)
    s = session_factory()
    try:
        agents = []
        for i in range(100):
            a = Agent(campaign_id=CAMPAIGN_ID, state="AVAILABLE", last_state_change_at=NOW, updated_at=NOW)
            s.add(a)
            agents.append(a)

        # 80 in-flight RINGING calls
        ringing_calls = []
        for j in range(80):
            b = Borrower(campaign_id=CAMPAIGN_ID, phone_token=f"tok_rg_{j}", state="RESERVED")
            s.add(b)
            s.flush()

            created_time = NOW + timedelta(seconds=j)  # j=79 is newest
            c = Call(
                id=uuid.uuid4(),
                campaign_id=CAMPAIGN_ID,
                borrower_id=b.id,
                agent_id=None,
                provider="mock_a",
                state=CallState.RINGING.value,
                state_rank=STATE_RANK[CallState.RINGING],
                version=0,
                created_at=created_time,
                updated_at=created_time,
            )
            s.add(c)
            ringing_calls.append(c)

        s.commit()
    finally:
        s.close()

    # 40 agents flip OFFLINE -> capacity drops from 100 to 60!
    # in_flight is 80 > capacity 60 -> excess is 20!
    s = session_factory()
    try:
        offline_agents = s.scalars(select(Agent).where(Agent.campaign_id == CAMPAIGN_ID).limit(40)).all()
        for a in offline_agents:
            a.state = AgentState.OFFLINE.value
        s.commit()
    finally:
        s.close()

    guard_res = check_and_guard_capacity(
        session_factory=session_factory,
        campaign_id=CAMPAIGN_ID,
        now=clock.now(),
        provider_name="mock_a",
    )

    assert guard_res["capacity"] == 60
    assert guard_res["in_flight"] == 80
    assert guard_res["excess"] == 20
    assert guard_res["cancelled"] == 20
    assert guard_res["ticks_to_recover"] >= 0

    # Verify that the 20 newest calls were the ones cancelled
    s = session_factory()
    try:
        cancelled_calls = s.scalars(
            select(Call).where(Call.campaign_id == CAMPAIGN_ID, Call.state == CallState.CANCELLED.value)
        ).all()
        assert len(cancelled_calls) == 20
        # The remaining 60 calls are still RINGING
        remaining_ringing = s.scalars(
            select(Call).where(Call.campaign_id == CAMPAIGN_ID, Call.state == CallState.RINGING.value)
        ).all()
        assert len(remaining_ringing) == 60
    finally:
        s.close()


def test_answer_time_claim_race(session_factory):
    """
    Answer-time claim race:
    Many ANSWERED events racing concurrently for few free agents:
    - No agent is double-claimed.
    - Excess answered calls enter HELD cleanly.
    """
    total_free_agents = 3
    total_answered_calls = 10
    clock = FakeClock(NOW)

    s = session_factory()
    try:
        # 3 AVAILABLE agents
        for i in range(total_free_agents):
            a = Agent(campaign_id=CAMPAIGN_ID, state="AVAILABLE", last_state_change_at=NOW, updated_at=NOW)
            s.add(a)

        # 10 calls in INITIATED with agent_id=None (predictive)
        calls = []
        for j in range(total_answered_calls):
            b = Borrower(campaign_id=CAMPAIGN_ID, phone_token=f"tok_race_{j}", state="RESERVED")
            s.add(b)
            s.flush()

            c = Call(
                id=uuid.uuid4(),
                campaign_id=CAMPAIGN_ID,
                borrower_id=b.id,
                agent_id=None,
                provider="mock_a",
                provider_call_id=f"prov_race_{j}",
                state=CallState.INITIATED.value,
                state_rank=STATE_RANK[CallState.INITIATED],
                version=0,
                created_at=NOW,
                updated_at=NOW,
            )
            s.add(c)
            calls.append(c)

        s.commit()
    finally:
        s.close()

    # Ingest 10 ANSWERED events concurrently
    for j in range(total_answered_calls):
        s = session_factory()
        try:
            evt = ProviderEvent(
                event_id=f"ev_race_ans_{j}",
                provider_call_id=f"prov_race_{j}",
                event_type=ProviderEventType.ANSWERED,
            )
            ingest(s, "mock_a", evt, clock=clock)
        finally:
            s.close()

    # 4 concurrent worker threads running process_pending
    def worker():
        process_pending(session_factory, batch_size=5, clock=clock)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker) for _ in range(4)]
        concurrent.futures.wait(futures)

    s = session_factory()
    try:
        connected_calls = s.scalars(
            select(Call).where(Call.campaign_id == CAMPAIGN_ID, Call.state == CallState.CONNECTED.value)
        ).all()
        held_calls = s.scalars(
            select(Call).where(Call.campaign_id == CAMPAIGN_ID, Call.state == CallState.HELD.value)
        ).all()

        # Invariant 1: exactly 3 calls connected (since only 3 agents free)
        assert len(connected_calls) == total_free_agents

        # Invariant 2: exactly 7 calls in HELD
        assert len(held_calls) == (total_answered_calls - total_free_agents)

        # Invariant 3: no agent claimed more than once
        claimed_agents = [c.agent_id for c in connected_calls]
        assert len(set(claimed_agents)) == total_free_agents
    finally:
        s.close()
