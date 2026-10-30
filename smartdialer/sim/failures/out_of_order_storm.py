"""
smartdialer.sim.failures.out_of_order_storm — Failure demonstration: Out-of-order event storm.

Demonstrates:
1. Provider callbacks arrive completely backwards: COMPLETED -> ANSWERED -> RINGING.
2. Call FSM rank ensures terminal absorption: COMPLETED transitions call to COMPLETED (rank 6).
3. The subsequent ANSWERED (rank 4) and RINGING (rank 3) events are cleanly marked IGNORED_LATE.
4. Call state is never demoted backwards and agents/borrowers are never double-released.
5. Invariants asserted.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.init_db import init_db
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.db.session import make_engine, make_session_factory, transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.events.inbox import ingest
from smartdialer.events.processor import process_pending
from smartdialer.providers.base import ProviderEvent, ProviderEventType
from smartdialer.sim.invariants import assert_all_invariants


def run_demo() -> None:
    db_url = os.environ.get("TEST_DB_URL", settings.test_db_url)
    init_db(db_url, drop=True)
    engine = make_engine(db_url)
    session_factory = make_session_factory(engine)

    t0 = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0)

    print("\n" + "=" * 70)
    print(" FAILURE DEMO 5: OUT-OF-ORDER EVENT STORM (COMPLETED -> ANSWERED -> RINGING)")
    print("=" * 70)

    # 1. Setup Call in INITIATED with Agent & Borrower
    call_id = uuid.uuid4()
    s = session_factory()
    try:
        with transaction(s):
            a = Agent(id=1, campaign_id=1, state=AgentState.DIALING.value, last_state_change_at=t0, updated_at=t0)
            b = Borrower(id=1, campaign_id=1, phone_token="tok_ooo_1", state="RESERVED", attempts=1)
            c = Call(
                id=call_id,
                campaign_id=1,
                borrower_id=1,
                agent_id=1,
                provider="mock_b",
                provider_call_id="prov_ooo_99",
                state=CallState.INITIATED.value,
                state_rank=STATE_RANK[CallState.INITIATED],
                version=0,
                created_at=t0,
                updated_at=t0,
            )
            s.add(a)
            s.add(b)
            s.add(c)
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] Call initialized in INITIATED (rank 2).")

    # 2. Ingest Events Backwards
    # Event 1: COMPLETED (Rank 6)
    evt_comp = ProviderEvent(event_id="ev_ooo_1_comp", provider_call_id="prov_ooo_99", event_type=ProviderEventType.COMPLETED)
    # Event 2: ANSWERED (Rank 4)
    evt_ans = ProviderEvent(event_id="ev_ooo_2_ans", provider_call_id="prov_ooo_99", event_type=ProviderEventType.ANSWERED)
    # Event 3: RINGING (Rank 3)
    evt_ring = ProviderEvent(event_id="ev_ooo_3_ring", provider_call_id="prov_ooo_99", event_type=ProviderEventType.RINGING)

    print(f"[{clock.now().strftime('%H:%M:%S')}] [OUT-OF-ORDER] Ingesting in reverse sequence: COMPLETED, then ANSWERED, then RINGING...")

    s = session_factory()
    try:
        ingest(s, "mock_b", evt_comp, clock=clock)
        ingest(s, "mock_b", evt_ans, clock=clock)
        ingest(s, "mock_b", evt_ring, clock=clock)
    finally:
        s.close()

    # 3. Process Pending Events sequentially
    print(f"[{clock.now().strftime('%H:%M:%S')}] Processing events through FSM pipeline...")
    processed = process_pending(session_factory, batch_size=10, clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Processed all 3 events.")
    assert processed == 3

    # 4. Verify Final State & Event Statuses
    s = session_factory()
    try:
        c = s.get(Call, call_id)
        a = s.get(Agent, 1)
        b = s.get(Borrower, 1)

        events = s.scalars(select(CallEvent).order_by(CallEvent.id)).all()
        for ev in events:
            print(f"       -> Event '{ev.event_type}': status = {ev.status} (reason = '{ev.reason}')")

        print(f"[{clock.now().strftime('%H:%M:%S')}] Final Call state: {c.state} (rank {c.state_rank})")
        print(f"[{clock.now().strftime('%H:%M:%S')}] Final Agent state: {a.state} (released to WRAP_UP)")
        print(f"[{clock.now().strftime('%H:%M:%S')}] Final Borrower state: {b.state} (marked DONE)")

        assert c.state == CallState.COMPLETED.value
        assert a.state == AgentState.WRAP_UP.value
        assert b.state == "DONE"

        # Event statuses
        comp_ev = next(e for e in events if e.event_type == "COMPLETED")
        ans_ev = next(e for e in events if e.event_type == "ANSWERED")
        ring_ev = next(e for e in events if e.event_type == "RINGING")

        assert comp_ev.status == "APPLIED"
        assert ans_ev.status == "IGNORED_LATE"
        assert ring_ev.status == "IGNORED_LATE"

        assert_all_invariants(session=s, campaign_id=1, now=clock.now(), max_inflight_observed=1, total_agents=1)
        print("-> [PASS] Invariants verified. Zero backwards state demotions.\n")
    finally:
        s.close()


if __name__ == "__main__":
    run_demo()
