"""
tests/test_concurrency_progressive.py — High concurrency progressive dialer invariant test.

Requirement:
- 50 AVAILABLE agents, 10 concurrent workers, progressive.
- Never more than 50 active agent-bound calls.
- Poll the DB during the run and assert the invariant continuously.
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower
from smartdialer.orchestrator.tick import run_tick
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.registry import register

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 42


def test_50_agents_10_workers_never_exceeds_50_active_calls(session_factory, db_engine):
    """
    50 AVAILABLE agents, 10 concurrent workers, progressive ->
    assert continuously during the run that active agent-bound calls <= 50.
    """
    total_agents = 50
    total_borrowers = 100

    # Setup database with 50 agents and 100 borrowers
    s = session_factory()
    try:
        for i in range(total_agents):
            a = Agent(
                campaign_id=CAMPAIGN_ID,
                state="AVAILABLE",
                last_state_change_at=NOW,
                updated_at=NOW,
            )
            s.add(a)

        for j in range(total_borrowers):
            b = Borrower(
                campaign_id=CAMPAIGN_ID,
                phone_token=f"tok_conc_{j}",
                state="ELIGIBLE",
                priority=j,
            )
            s.add(b)

        s.commit()
    finally:
        s.close()

    provider = MockProviderA(clock=FakeClock(NOW))
    register(provider)

    stop_poller = threading.Event()
    poller_exceptions = []
    max_active_seen = 0

    def poller():
        nonlocal max_active_seen
        poll_session = session_factory()
        try:
            while not stop_poller.is_set():
                # Count active calls bound to an agent
                active_count = poll_session.execute(
                    text("""
                        SELECT COUNT(*) FROM calls
                        WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED')
                          AND agent_id IS NOT NULL
                    """)
                ).scalar() or 0

                if active_count > max_active_seen:
                    max_active_seen = active_count

                # INVARIANT: Never more than 50 active calls
                if active_count > total_agents:
                    poller_exceptions.append(
                        f"INVARIANT VIOLATED: {active_count} active calls > {total_agents} agents!"
                    )

                # INVARIANT: No agent with multiple active calls
                agent_duplicates = poll_session.execute(
                    text("""
                        SELECT agent_id, COUNT(*)
                        FROM calls
                        WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED')
                          AND agent_id IS NOT NULL
                        GROUP BY agent_id
                        HAVING COUNT(*) > 1
                    """)
                ).fetchall()
                if agent_duplicates:
                    poller_exceptions.append(f"Agent assigned multiple active calls: {agent_duplicates}")

                time.sleep(0.01)
        except Exception as e:
            poller_exceptions.append(str(e))
        finally:
            poll_session.close()

    # Start poller thread
    poller_thread = threading.Thread(target=poller, daemon=True)
    poller_thread.start()

    # Run 10 worker threads performing ticks concurrently
    num_workers = 10
    ticks_per_worker = 5

    def worker(worker_id):
        for _ in range(ticks_per_worker):
            run_tick(
                session_factory=session_factory,
                campaign_id=CAMPAIGN_ID,
                provider_name="mock_a",
            )
            time.sleep(0.01)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [pool.submit(worker, i) for i in range(num_workers)]
        concurrent.futures.wait(futures)

    # Stop poller
    stop_poller.set()
    poller_thread.join(timeout=5.0)

    # Check for any invariant violations detected by the continuous poller
    assert not poller_exceptions, f"Poller reported invariant violations: {poller_exceptions}"
    assert max_active_seen <= total_agents, f"Max active calls was {max_active_seen} (> {total_agents})"
