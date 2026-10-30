"""
smartdialer.sim.failures.provider_outage — Failure demonstration: Provider outage mid-run.

Demonstrates:
1. Normal operation with MockProviderA.
2. Sudden provider outage injected mid-run.
3. Health monitor / circuit breaker trips to OPEN.
4. Safety Rule R4 intercepts and strictly REJECTS new dial proposals.
5. In-flight calls drain cleanly.
6. Outage ends; circuit breaker tests HALF_OPEN, succeeds, and resumes dials.
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
from smartdialer.providers.health import CircuitBreaker, CircuitState
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.registry import clear as clear_registry, register as register_provider
from smartdialer.safety.controller import SafetyAction
from smartdialer.sim.invariants import assert_all_invariants


def run_demo() -> None:
    db_url = os.environ.get("TEST_DB_URL", settings.test_db_url)
    init_db(db_url, drop=True)
    engine = make_engine(db_url)
    session_factory = make_session_factory(engine)

    t0 = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(t0)

    print("\n" + "=" * 70)
    print(" FAILURE DEMO 2: PROVIDER OUTAGE, CIRCUIT BREAKER & SAFETY RULE R4")
    print("=" * 70)

    clear_registry()
    provider = MockProviderA(clock=clock, rejection_rate=0.0)
    register_provider(provider)

    # 1. Populate 10 Agents & Borrowers
    s = session_factory()
    try:
        with transaction(s):
            for i in range(1, 11):
                s.add(Agent(id=i, campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=t0, updated_at=t0))
                s.add(Borrower(id=i, campaign_id=1, phone_token=f"tok_{i}", state="ELIGIBLE", attempts=0, next_eligible_at=t0))
    finally:
        s.close()

    print(f"[{clock.now().strftime('%H:%M:%S')}] 10 agents available. Running baseline tick...")
    summary1 = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PROGRESSIVE", clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Baseline: Dialed {summary1['dialed_calls']} calls, Advisory={summary1['advisory_action']}.")

    # 2. Inject Outage
    print(f"[{clock.now().strftime('%H:%M:%S')}] [FAILURE INJECTION] Setting MockProviderA into OUTAGE mode!")
    provider.set_outage(True)

    breaker = CircuitBreaker("mock_a", clock=clock)
    for _ in range(settings.circuit_breaker_min_calls):
        breaker.record(success=False, latency_ms=1000.0)
    assert breaker.state == CircuitState.OPEN
    provider.health_snapshot = breaker.snapshot
    snap = breaker.snapshot()
    failure_rate = snap["health"]["failure_rate"]
    print(f"[{clock.now().strftime('%H:%M:%S')}] Circuit breaker tripped to OPEN (failure rate={failure_rate * 100:.0f}%).")




    # 3. Next tick under outage
    clock.advance(seconds=1)
    summary2 = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PROGRESSIVE", clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Outage Tick: Safety Action = {summary2['advisory_action']} (Enforced = {summary2['enforced_action']})")
    print(f"[{clock.now().strftime('%H:%M:%S')}] Approved calls = {summary2['approved_calls']}, Dialed calls = {summary2['dialed_calls']}.")

    assert summary2["advisory_action"] == SafetyAction.REJECT.value
    assert summary2["approved_calls"] == 0
    assert summary2["dialed_calls"] == 0
    print(f"[{clock.now().strftime('%H:%M:%S')}] -> Rule R4 strictly blocked all dialing!")

    # 4. Outage Cleared
    print(f"[{clock.now().strftime('%H:%M:%S')}] [RECOVERY] Provider outage ends. Restoring healthy telephony.")
    provider.set_outage(False)
    clock.advance(seconds=settings.circuit_breaker_cooldown_seconds + 1)

    # Allow call transitions from OPEN to HALF_OPEN after cooldown
    assert breaker.allow_call() is True
    assert breaker.state == CircuitState.HALF_OPEN

    # Record successful probe call to close circuit
    breaker.record(success=True, latency_ms=50.0)
    assert breaker.state == CircuitState.CLOSED
    provider.health_snapshot = breaker.snapshot


    summary3 = run_tick(session_factory, campaign_id=1, provider_name="mock_a", mode="PROGRESSIVE", clock=clock)
    print(f"[{clock.now().strftime('%H:%M:%S')}] Recovery Tick: Safety Action = {summary3['advisory_action']}.")
    print(f"[{clock.now().strftime('%H:%M:%S')}] Circuit CLOSED. Dials resumed: {summary3['dialed_calls']} calls allocated.")

    s = session_factory()
    try:
        assert_all_invariants(session=s, campaign_id=1, now=clock.now(), max_inflight_observed=10, total_agents=10)
        print("-> [PASS] Invariants verified. Provider outage handled gracefully.\n")
    finally:
        s.close()
        clear_registry()



if __name__ == "__main__":
    run_demo()
