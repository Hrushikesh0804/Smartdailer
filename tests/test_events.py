"""
tests/test_events.py — Integration tests for event inbox and event processor.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.db.session import make_session_factory, transaction
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.events.inbox import ingest
from smartdialer.events.processor import process_pending
from smartdialer.providers.base import ProviderEvent, ProviderEventType

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _create_call(session, state=CallState.INITIATED, provider_call_id="prov_call_123"):
    b = Borrower(
        campaign_id=CAMPAIGN_ID,
        phone_token="tok_123",
        state="RESERVED",
        attempts=1,
    )
    session.add(b)
    session.flush()

    a = Agent(
        campaign_id=CAMPAIGN_ID,
        state="DIALING",
        last_state_change_at=NOW,
        updated_at=NOW,
    )
    session.add(a)
    session.flush()

    c = Call(
        id=uuid.uuid4(),
        campaign_id=CAMPAIGN_ID,
        borrower_id=b.id,
        agent_id=a.id,
        provider="mock_a",
        provider_call_id=provider_call_id,
        state=state.value,
        state_rank=STATE_RANK[state],
        version=0,
        created_at=NOW,
        updated_at=NOW,
    )
    session.add(c)
    session.commit()
    return c


def test_answered_x3_completed(session_factory, db_engine):
    """
    ANSWERED x3, COMPLETED -> exactly one APPLIED ANSWERED row,
    two IGNORED_DUPLICATE, final COMPLETED.
    """
    s = session_factory()
    try:
        call = _create_call(s, state=CallState.INITIATED, provider_call_id="prov_1")
        call_id = call.id
    finally:
        s.close()

    clock = FakeClock(NOW)

    # Ingest 3 distinct ANSWERED events (e.g. repeated callbacks with unique event IDs)
    for i in range(1, 4):
        s = session_factory()
        try:
            evt = ProviderEvent(
                event_id=f"ev_ans_{i}",
                provider_call_id="prov_1",
                event_type=ProviderEventType.ANSWERED,
            )
            ingest(s, "mock_a", evt, clock=clock)
        finally:
            s.close()

    # Ingest 1 COMPLETED event
    s = session_factory()
    try:
        evt = ProviderEvent(
            event_id="ev_comp_1",
            provider_call_id="prov_1",
            event_type=ProviderEventType.COMPLETED,
        )
        ingest(s, "mock_a", evt, clock=clock)
    finally:
        s.close()

    # Process pending events
    processed = process_pending(session_factory, batch_size=10, clock=clock)
    assert processed == 4

    # Verify call_events table
    s = session_factory()
    try:
        events = s.scalars(
            select(CallEvent).order_by(CallEvent.id)
        ).all()
        assert len(events) == 4

        ans_events = [e for e in events if e.event_type == "ANSWERED"]
        comp_events = [e for e in events if e.event_type == "COMPLETED"]

        applied_ans = [e for e in ans_events if e.status == "APPLIED"]
        dup_ans = [e for e in ans_events if e.status == "IGNORED_DUPLICATE"]

        assert len(applied_ans) == 1
        assert len(dup_ans) == 2
        assert len(comp_events) == 1
        assert comp_events[0].status == "APPLIED"

        # Check final call state
        final_call = s.get(Call, call_id)
        assert final_call.state == CallState.COMPLETED.value
    finally:
        s.close()


def test_completed_answered_ringing_out_of_order(session_factory):
    """
    COMPLETED, ANSWERED, RINGING -> final COMPLETED, later two IGNORED_LATE.
    """
    s = session_factory()
    try:
        call = _create_call(s, state=CallState.INITIATED, provider_call_id="prov_ooo")
        call_id = call.id
    finally:
        s.close()

    clock = FakeClock(NOW)

    # Ingest COMPLETED first
    s = session_factory()
    try:
        evt_comp = ProviderEvent(
            event_id="ev_c",
            provider_call_id="prov_ooo",
            event_type=ProviderEventType.COMPLETED,
        )
        ingest(s, "mock_a", evt_comp, clock=clock)
    finally:
        s.close()

    process_pending(session_factory, batch_size=10, clock=clock)

    # Ingest late ANSWERED and RINGING
    for ev_id, ev_type in [("ev_a", ProviderEventType.ANSWERED), ("ev_r", ProviderEventType.RINGING)]:
        s = session_factory()
        try:
            evt = ProviderEvent(
                event_id=ev_id,
                provider_call_id="prov_ooo",
                event_type=ev_type,
            )
            ingest(s, "mock_a", evt, clock=clock)
        finally:
            s.close()

    process_pending(session_factory, batch_size=10, clock=clock)

    s = session_factory()
    try:
        call = s.get(Call, call_id)
        assert call.state == CallState.COMPLETED.value

        events = s.scalars(select(CallEvent).order_by(CallEvent.id)).all()
        assert len(events) == 3

        comp_ev = [e for e in events if e.event_type == "COMPLETED"][0]
        late_evs = [e for e in events if e.event_type in ("ANSWERED", "RINGING")]

        assert comp_ev.status == "APPLIED"
        assert len(late_evs) == 2
        assert all(e.status == "IGNORED_LATE" for e in late_evs)
    finally:
        s.close()


def test_same_event_id_concurrent_ingest(session_factory):
    """
    Same event_id delivered 10 times concurrently from 4 threads -> exactly one row in DB and exactly one APPLIED.
    """
    s = session_factory()
    try:
        call = _create_call(s, state=CallState.INITIATED, provider_call_id="prov_conc")
        call_id = call.id
    finally:
        s.close()

    clock = FakeClock(NOW)
    evt = ProviderEvent(
        event_id="ev_shared_id_100",
        provider_call_id="prov_conc",
        event_type=ProviderEventType.ANSWERED,
    )

    def worker():
        local_s = session_factory()
        try:
            ingest(local_s, "mock_a", evt, clock=clock)
        finally:
            local_s.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker) for _ in range(10)]
        concurrent.futures.wait(futures)

    # Process events
    process_pending(session_factory, batch_size=10, clock=clock)

    s = session_factory()
    try:
        events = s.scalars(
            select(CallEvent).where(CallEvent.event_id == "ev_shared_id_100")
        ).all()
        # Primary key/unique constraint guaranteed exactly 1 row was stored
        assert len(events) == 1
        assert events[0].status == "APPLIED"
    finally:
        s.close()


def test_crash_simulated_after_ingest_before_processing(session_factory):
    """
    Crash simulated after ingest, before processing -> replay applies once.
    """
    s = session_factory()
    try:
        call = _create_call(s, state=CallState.INITIATED, provider_call_id="prov_crash")
        call_id = call.id
    finally:
        s.close()

    clock = FakeClock(NOW)
    evt = ProviderEvent(
        event_id="ev_durable",
        provider_call_id="prov_crash",
        event_type=ProviderEventType.ANSWERED,
    )

    # Ingest commits to DB
    s = session_factory()
    try:
        ingest(s, "mock_a", evt, clock=clock)
    finally:
        s.close()

    # Simulate crash: do nothing, sessions were closed.
    # Now simulate restart / next tick:
    count1 = process_pending(session_factory, batch_size=10, clock=clock)
    assert count1 == 1

    # Second pass: already applied, nothing left to process
    count2 = process_pending(session_factory, batch_size=10, clock=clock)
    assert count2 == 0

    s = session_factory()
    try:
        ev = s.scalars(select(CallEvent).where(CallEvent.event_id == "ev_durable")).one()
        assert ev.status == "APPLIED"
        c = s.get(Call, call_id)
        assert c.state == CallState.ANSWERED.value
    finally:
        s.close()


def test_unknown_call_event_quarantined_then_applied(session_factory):
    """
    Unknown-call event quarantined, then applied after the call row appears.
    """
    clock = FakeClock(NOW)
    prov_call_id = "prov_future_call_999"

    evt = ProviderEvent(
        event_id="ev_early_arrival",
        provider_call_id=prov_call_id,
        event_type=ProviderEventType.RINGING,
    )

    # Ingest event before call exists in DB
    s = session_factory()
    try:
        ingest(s, "mock_a", evt, clock=clock)
    finally:
        s.close()

    # First process attempt: call row does not exist -> QUARANTINED
    process_pending(session_factory, batch_size=10, clock=clock)

    s = session_factory()
    try:
        ev = s.scalars(select(CallEvent).where(CallEvent.event_id == "ev_early_arrival")).one()
        assert ev.status == "QUARANTINED"
        assert ev.attempts == 1
        assert ev.next_retry_at is not None
    finally:
        s.close()

    # Now the call row appears (e.g. Executor Txn 2 commits)
    s = session_factory()
    try:
        call = _create_call(s, state=CallState.INITIATED, provider_call_id=prov_call_id)
        call_id = call.id
    finally:
        s.close()

    # Advance clock past retry backoff
    clock.advance(seconds=10)

    # Process again: should claim QUARANTINED event and apply it
    process_pending(session_factory, batch_size=10, clock=clock)

    s = session_factory()
    try:
        ev = s.scalars(select(CallEvent).where(CallEvent.event_id == "ev_early_arrival")).one()
        assert ev.status == "APPLIED"
        assert ev.call_id == call_id

        call = s.get(Call, call_id)
        assert call.state == CallState.RINGING.value
    finally:
        s.close()
