"""
smartdialer.sim.failures.worker_crash — Failure demonstration: Worker crash after reservation.

Demonstrates:
1. Worker allocates and reserves a call (Call=RESERVED, Agent=RESERVED).
2. Worker process simulated crash before provider call.
3. Time advances past call lease expiry (call_lease_ttl_seconds).
4. Sweeper discovers the expired lease, cancels the orphaned call, and releases the agent.
5. Invariants asserted and timeline printed.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.init_db import init_db
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import make_engine, make_session_factory, transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.recovery.sweeper import Sweeper
from smartdialer.sim.invariants import assert_all_invariants


def run_demo() -> None:
    db_url = os.environ.get("TEST_DB_URL", settings.test_db_url)
    init_db(db_url, drop=True)
    engine = make_engine(db_url)
    session_factory = make_session_factory(engine)

    t0 = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0)
    sweeper = Sweeper(session_factory, clock=clock)

    print("\n" + "=" * 70)
    print(" FAILURE DEMO 1: WORKER CRASH AFTER RESERVATION & SWEEPER RECOVERY")
    print("=" * 70)

    # 1. Setup Agent & Borrower
    s = session_factory()
    try:
        with transaction(s):
            agent = Agent(
                id=1,
                campaign_id=1,
                state=AgentState.AVAILABLE.value,
                last_state_change_at=t0,
                updated_at=t0,
            )
            borrower = Borrower(
                id=1,
                campaign_id=1,
                phone_token="tok_crash_1",
                state="ELIGIBLE",
                attempts=0,
                next_eligible_at=t0,
            )
            s.add(agent)
            s.add(borrower)
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] Agent 1 and Borrower 1 initialized.")

    # 2. Worker reserves Call
    call_id = uuid.uuid4()
    s = session_factory()
    try:
        with transaction(s):
            a = s.get(Agent, 1)
            b = s.get(Borrower, 1)
            a.state = AgentState.RESERVED.value
            b.state = "RESERVED"
            call = Call(
                id=call_id,
                campaign_id=1,
                borrower_id=b.id,
                agent_id=a.id,
                provider="mock_a",
                state=CallState.RESERVED.value,
                state_rank=STATE_RANK[CallState.RESERVED],
                lease_expires_at=t0 + timedelta(seconds=settings.call_lease_ttl_seconds),
                created_at=t0,
                updated_at=t0,
            )
            s.add(call)
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] Worker reserved Call {str(call_id)[:8]} (lease={settings.call_lease_ttl_seconds}s).")

    # 3. Simulate Worker Crash
    print(f"[{clock.now().strftime('%H:%M:%S')}] [CRASH] Worker process dies abruptly before dialing provider!")
    print(f"[{clock.now().strftime('%H:%M:%S')}] State: Call=RESERVED, Agent=RESERVED (orphaned).")

    # 4. Advance time past lease expiration
    clock.advance(seconds=settings.call_lease_ttl_seconds + 5)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Clock advanced past lease expiration deadline.")

    # 5. Sweeper runs
    print(f"[{clock.now().strftime('%H:%M:%S')}] Sweeper running recovery pass...")
    counters = sweeper.sweep_once()
    print(f"[{clock.now().strftime('%H:%M:%S')}] Sweeper action counters: {counters}")

    # 6. Verify Recovery
    s = session_factory()
    try:
        recovered_call = s.get(Call, call_id)
        recovered_agent = s.get(Agent, 1)
        recovered_borrower = s.get(Borrower, 1)

        print(f"[{clock.now().strftime('%H:%M:%S')}] Post-recovery Call state: {recovered_call.state} ({recovered_call.terminal_reason})")
        print(f"[{clock.now().strftime('%H:%M:%S')}] Post-recovery Agent state: {recovered_agent.state}")
        print(f"[{clock.now().strftime('%H:%M:%S')}] Post-recovery Borrower state: {recovered_borrower.state}")

        assert recovered_call.state == CallState.CANCELLED.value
        assert "lease expired" in recovered_call.terminal_reason
        assert recovered_agent.state == AgentState.AVAILABLE.value

        assert recovered_borrower.state == "ELIGIBLE"

        assert_all_invariants(
            session=s,
            campaign_id=1,
            now=clock.now(),
            max_inflight_observed=1,
            total_agents=1,
        )
        print("-> [PASS] Invariants verified. Zero orphaned locks remain.\n")
    finally:
        s.close()


if __name__ == "__main__":
    run_demo()
