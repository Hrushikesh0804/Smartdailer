"""
smartdialer.sim.failures.answer_spike — Failure demonstration: Answer-rate spike under predictive pacing.

Demonstrates:
1. Campaign running predictive pacing at low answer rate (10%).
2. Sudden answer-rate spike to 70%.
3. Estimator dual-window drift guard (p_hi = max(p_hi_long, p_hi_recent)) reacts instantly.
4. Pacing proposal drops from aggressive overdialing to conservative dials.
5. Safety Controller enforces caps; prevents unbounded abandonment.
6. Invariants asserted.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.db.init_db import init_db
from smartdialer.db.models import Agent, Borrower
from smartdialer.db.session import make_engine, make_session_factory, transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.orchestrator.tick import run_tick
from smartdialer.pacing.estimator import predict_answer_rate, record_dial_outcome, reset_estimator
from smartdialer.safety.controller import reset_hysteresis

from smartdialer.sim.invariants import assert_all_invariants


def run_demo() -> None:
    db_url = os.environ.get("TEST_DB_URL", settings.test_db_url)
    init_db(db_url, drop=True)
    engine = make_engine(db_url)
    session_factory = make_session_factory(engine)

    t0 = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0)
    reset_estimator()
    reset_hysteresis()

    print("\n" + "=" * 70)
    print(" FAILURE DEMO 6: SUDDEN ANSWER-RATE SPIKE (10% -> 70%) UNDER PREDICTIVE")
    print("=" * 70)

    # 1. Setup 20 Agents & Borrowers
    s = session_factory()
    try:
        with transaction(s):
            for i in range(1, 21):
                s.add(Agent(id=i, campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=t0, updated_at=t0))
            for j in range(1, 101):
                s.add(Borrower(id=j, campaign_id=1, phone_token=f"tok_spike_{j}", state="ELIGIBLE", attempts=0, next_eligible_at=t0))
    finally:
        s.close()

    # 2. Establish 10% baseline in estimator
    print(f"[{clock.now().strftime('%H:%M:%S')}] Seeding baseline observations: 90 unanswered, 10 answered (10% rate)...")
    for _ in range(90):
        record_dial_outcome(1, answered=False, clock=clock)
    for _ in range(10):
        record_dial_outcome(1, answered=True, clock=clock)

    est_baseline = predict_answer_rate(1, clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Baseline Estimator: p_mean = {est_baseline.p_mean:.2f}, p_hi = {est_baseline.p_hi:.2f}")

    # Baseline tick with 20 free agents
    summary1 = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PREDICTIVE", clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Baseline Pacing Proposal: requested {summary1['requested_calls']} calls for 20 capacity (overdialing for 10% rate).")

    # 3. Sudden Surge: Next 25 dials have 20 answers (80% answer spike!)
    print(f"[{clock.now().strftime('%H:%M:%S')}] [SPIKE INJECTION] Sudden surge: 20 answers out of next 25 dials (~80% answer burst)!")
    for _ in range(20):
        clock.advance(seconds=1)
        record_dial_outcome(1, answered=True, clock=clock)
    for _ in range(5):
        clock.advance(seconds=1)
        record_dial_outcome(1, answered=False, clock=clock)

    est_spiked = predict_answer_rate(1, clock=clock)

    print(f"[{clock.now().strftime('%H:%M:%S')}] Post-Spike Estimator:")
    print(f"       -> p_mean: {est_spiked.p_mean:.2f}")
    print(f"       -> p_hi: {est_spiked.p_hi:.2f} (Recent window + Drift Guard raised upper bound)")

    assert est_spiked.p_hi > 0.40  # dramatically increased

    # 4. Next Pacing Tick under Spiked Estimator
    summary2 = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PREDICTIVE", clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Post-Spike Pacing Proposal:")
    print(f"       -> Requested calls: {summary2['requested_calls']} (Defensively dialed down from {summary1['requested_calls']})")
    print(f"       -> Advisory Action: {summary2['advisory_action']} ({summary2['approved_calls']} approved)")
    print(f"       -> Rationale: {summary2.get('pacing_why', '')}")

    assert summary2["requested_calls"] < summary1["requested_calls"]

    s = session_factory()
    try:
        assert_all_invariants(session=s, campaign_id=1, now=clock.now(), max_inflight_observed=summary1['requested_calls'], total_agents=20)
        print("-> [PASS] Invariants verified. Drift guard defensively curtailed overdialing.\n")
    finally:
        s.close()


if __name__ == "__main__":
    run_demo()
