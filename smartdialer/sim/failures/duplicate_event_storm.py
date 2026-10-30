"""
smartdialer.sim.failures.duplicate_event_storm — Failure demonstration: Duplicate-event storm.

Demonstrates:
1. Telephony webhook sends 200 duplicate callbacks for the same call event from 8 concurrent worker threads.
2. Ingestion layer leverages INSERT ... ON CONFLICT DO NOTHING to drop duplicates at DB level.
3. Event processor executes idempotent state transitions.
4. Exactly 1 row applied, zero double-incremented agent or borrower states.
5. Invariants asserted.
"""

from __future__ import annotations

import concurrent.futures
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select, func

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
    print(" FAILURE DEMO 4: CONCURRENT DUPLICATE-EVENT STORM (200 DUPLICATES)")
    print("=" * 70)

    # 1. Setup 1 Agent, 1 Borrower, 1 Call in INITIATED
    call_id = uuid.uuid4()
    s = session_factory()
    try:
        with transaction(s):
            a = Agent(id=1, campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=t0, updated_at=t0)
            b = Borrower(id=1, campaign_id=1, phone_token="tok_storm_1", state="RESERVED", attempts=1)
            c = Call(
                id=call_id,
                campaign_id=1,
                borrower_id=1,
                agent_id=None,
                provider="mock_a",
                provider_call_id="prov_storm_123",
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

    print(f"[{clock.now().strftime('%H:%M:%S')}] Call {str(call_id)[:8]} created in INITIATED with provider_call_id='prov_storm_123'.")

    # 2. Duplicate storm: 200 concurrent ingestions of identical event_id
    storm_size = 200
    shared_event_id = "ev_storm_answered_999"
    evt = ProviderEvent(
        event_id=shared_event_id,
        provider_call_id="prov_storm_123",
        event_type=ProviderEventType.ANSWERED,
    )

    print(f"[{clock.now().strftime('%H:%M:%S')}] [STORM] Ingesting {storm_size} duplicate events concurrently across 8 threads...")

    def ingest_worker():
        local_s = session_factory()
        try:
            return ingest(local_s, "mock_a", evt, clock=clock)
        finally:
            local_s.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: ingest_worker(), range(storm_size)))

    inserted_count = sum(1 for r in results if r is True)
    dropped_count = sum(1 for r in results if r is False)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Ingest Results: {inserted_count} newly inserted, {dropped_count} deduplicated and dropped.")

    assert inserted_count == 1
    assert dropped_count == storm_size - 1

    # 3. Process Pending Events
    print(f"[{clock.now().strftime('%H:%M:%S')}] Processing pending events queue...")
    processed = process_pending(session_factory, batch_size=50, clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Event processor applied {processed} event(s).")
    assert processed == 1

    # 4. Invariants and DB verification
    s = session_factory()
    try:
        final_call = s.get(Call, call_id)
        final_agent = s.get(Agent, 1)
        event_rows = s.scalars(select(CallEvent).where(CallEvent.event_id == shared_event_id)).all()

        print(f"[{clock.now().strftime('%H:%M:%S')}] Call state: {final_call.state} (agent_id={final_call.agent_id})")
        print(f"[{clock.now().strftime('%H:%M:%S')}] Agent state: {final_agent.state}")
        print(f"[{clock.now().strftime('%H:%M:%S')}] call_events rows in DB: {len(event_rows)} (status={event_rows[0].status})")

        assert len(event_rows) == 1
        assert event_rows[0].status == "APPLIED"
        assert final_call.state == CallState.CONNECTED.value
        assert final_agent.state == AgentState.CONNECTED.value

        assert_all_invariants(session=s, campaign_id=1, now=clock.now(), max_inflight_observed=1, total_agents=1)
        print("-> [PASS] Invariants verified. Zero duplicates bypassed storage and zero side-effects doubled.\n")
    finally:
        s.close()


if __name__ == "__main__":
    run_demo()
