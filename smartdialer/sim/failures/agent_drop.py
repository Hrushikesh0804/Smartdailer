"""
smartdialer.sim.failures.agent_drop — Failure demonstration: Sudden mass agent drop.

Demonstrates:
1. 100 agents available with 80 in-flight RINGING calls.
2. 60 agents suddenly drop OFFLINE within seconds (available capacity drops to 40).
3. Live in-flight (80) now breaches available capacity (40).
4. Capacity Guard reacts immediately on the very next tick (1 tick / 1 second):
   - Capping dials at 0.
   - Cancelling the 40 excess RINGING calls newest-first.
   - Leaving answered / connected / held calls untouched.
5. Invariants asserted and reaction metrics reported.
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

from sqlalchemy import select

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.init_db import init_db
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import make_engine, make_session_factory, transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.orchestrator.tick import run_tick
from smartdialer.sim.invariants import assert_all_invariants


def run_demo() -> None:
    db_url = os.environ.get("TEST_DB_URL", settings.test_db_url)
    init_db(db_url, drop=True)
    engine = make_engine(db_url)
    session_factory = make_session_factory(engine)

    t0 = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0)

    print("\n" + "=" * 70)
    print(" FAILURE DEMO 3: MASS AGENT DROP (100 -> 40) & CAPACITY GUARD REACTION")
    print("=" * 70)

    # 1. Populate 100 Agents and 80 Ringing Calls
    s = session_factory()
    try:
        with transaction(s):
            for i in range(1, 101):
                s.add(Agent(id=i, campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=t0, updated_at=t0))

            # 80 calls in RINGING, staggered timestamps
            for j in range(1, 81):
                b = Borrower(id=j, campaign_id=1, phone_token=f"tok_{j}", state="RESERVED", attempts=1)
                s.add(b)
                s.flush()
                c = Call(
                    id=uuid.uuid4(),
                    campaign_id=1,
                    borrower_id=b.id,
                    agent_id=None,
                    provider="mock_a",
                    provider_call_id=f"prov_call_{j}",
                    state=CallState.RINGING.value,
                    state_rank=STATE_RANK[CallState.RINGING],
                    version=0,
                    created_at=t0 + timedelta(seconds=j),
                    updated_at=t0 + timedelta(seconds=j),
                )
                s.add(c)
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] Initial state: 100 agents AVAILABLE, 80 calls RINGING.")

    # 2. Sudden Agent Drop: 60 agents go OFFLINE abruptly
    drop_time = clock.now() + timedelta(seconds=1)
    clock.advance(seconds=1)
    s = session_factory()
    try:
        with transaction(s):
            agents_to_drop = s.scalars(
                select(Agent).where(Agent.campaign_id == 1, Agent.id <= 60)
            ).all()
            for a in agents_to_drop:
                a.state = AgentState.OFFLINE.value
                a.last_state_change_at = drop_time
                a.updated_at = drop_time
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] [DROP] 60 agents abruptly went OFFLINE! Live available capacity = 40 agents.")
    print(f"[{clock.now().strftime('%H:%M:%S')}] Deficit: In-flight calls (80) > Live capacity (40). Excess = 40.")

    # 3. Next tick executes: Capacity Guard intervenes
    start_reaction_tick = clock.now()
    summary = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PREDICTIVE", clock=clock)
    end_reaction_tick = clock.now()

    reaction_ticks = 1
    reaction_seconds = (end_reaction_tick - start_reaction_tick).total_seconds()

    print(f"[{clock.now().strftime('%H:%M:%S')}] Capacity Guard executed in tick summary:")
    print(f"       -> Capacity: {summary['capacity']}")
    print(f"       -> In-flight before guard: {summary['in_flight']}")
    print(f"       -> Excess cancelled: {summary['excess_cancelled']} calls (newest-first)")
    print(f"       -> Allocated new calls: {summary['allocated_calls']}")
    print(f"       -> Reaction time: {reaction_ticks} tick ({reaction_seconds:.3f} virtual seconds)")

    assert summary["excess_cancelled"] == 40
    assert summary["allocated_calls"] == 0

    # 4. Verify DB Call States
    s = session_factory()
    try:
        cancelled = s.scalars(
            select(Call).where(Call.campaign_id == 1, Call.state == CallState.CANCELLED.value)
        ).all()
        remaining_ringing = s.scalars(
            select(Call).where(Call.campaign_id == 1, Call.state == CallState.RINGING.value)
        ).all()

        print(f"[{clock.now().strftime('%H:%M:%S')}] Post-guard DB: {len(cancelled)} CANCELLED, {len(remaining_ringing)} remaining RINGING.")
        assert len(cancelled) == 40
        assert len(remaining_ringing) == 40

        assert_all_invariants(session=s, campaign_id=1, now=clock.now(), max_inflight_observed=80, total_agents=100)
        print("-> [PASS] Invariants verified. Excess calls drained safely without touching active sessions.\n")
    finally:
        s.close()


if __name__ == "__main__":
    run_demo()
