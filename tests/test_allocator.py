"""
tests/test_allocator.py — Integration tests for the allocator.

All tests run against the real PostgreSQL test database.

Test inventory
--------------
1. test_basic_allocation       - Happy path: 3 agents, 10 borrowers -> 3 calls.
2. test_race_2_threads         - 2 workers compete for 5 agents, 50 borrowers.
3. test_race_8_threads         - 8 workers compete for 5 agents, 50 borrowers.
4. test_constraint_backstop    - Bypass allocator; raw SQL violates partial index.
5. test_rollback_no_leak       - Exception mid-transaction; no leaked RESERVED rows.
6. test_no_agents_returns_empty - Allocator returns [] when no agents available.
7. test_no_borrowers_returns_empty - Allocator returns [] when no borrowers available.
8. test_max_n_respected        - max_n=2 with 5 agents -> at most 2 calls.
9. test_cooldown_respected     - Borrower with future next_eligible_at is skipped.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from smartdialer.allocator.allocator import release_reservation, reserve_for_progressive
from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import make_session_factory, transaction

CAMPAIGN_ID = 1
NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def make_agent(session, campaign_id=CAMPAIGN_ID, state="AVAILABLE", agent_id=None):
    a = Agent(
        id=agent_id,
        campaign_id=campaign_id,
        state=state,
        version=0,
        last_state_change_at=NOW,
        updated_at=NOW,
    )
    session.add(a)
    return a


def make_borrower(session, campaign_id=CAMPAIGN_ID, state="ELIGIBLE",
                  priority=0, next_eligible_at=None):
    b = Borrower(
        campaign_id=campaign_id,
        phone_token=f"tok_{uuid.uuid4().hex[:8]}",
        state=state,
        attempts=0,
        next_eligible_at=next_eligible_at,
        priority=priority,
    )
    session.add(b)
    return b


# ------------------------------------------------------------------ #
# Basic allocation                                                     #
# ------------------------------------------------------------------ #

def test_basic_allocation(session):
    """3 agents, 10 borrowers -> exactly 3 calls created, states updated."""
    for _ in range(3):
        make_agent(session)
    for _ in range(10):
        make_borrower(session)
    session.commit()

    with transaction(session):
        call_ids = reserve_for_progressive(session, CAMPAIGN_ID, max_n=10, now=NOW)

    assert len(call_ids) == 3

    # Verify DB state
    agents = session.query(Agent).all()
    reserved_agents = [a for a in agents if a.state == "RESERVED"]
    assert len(reserved_agents) == 3

    borrowers = session.query(Borrower).all()
    reserved_borrowers = [b for b in borrowers if b.state == "RESERVED"]
    assert len(reserved_borrowers) == 3

    calls = session.query(Call).all()
    assert len(calls) == 3
    assert all(c.state == "RESERVED" for c in calls)


def test_no_agents_returns_empty(session):
    for _ in range(5):
        make_borrower(session)
    session.commit()

    with transaction(session):
        call_ids = reserve_for_progressive(session, CAMPAIGN_ID, max_n=5, now=NOW)
    assert call_ids == []


def test_no_borrowers_returns_empty(session):
    for _ in range(5):
        make_agent(session)
    session.commit()

    with transaction(session):
        call_ids = reserve_for_progressive(session, CAMPAIGN_ID, max_n=5, now=NOW)
    assert call_ids == []


def test_max_n_respected(session):
    """Even if 5 agents are available, max_n=2 caps allocations at 2."""
    for _ in range(5):
        make_agent(session)
    for _ in range(5):
        make_borrower(session)
    session.commit()

    with transaction(session):
        call_ids = reserve_for_progressive(session, CAMPAIGN_ID, max_n=2, now=NOW)

    assert len(call_ids) == 2


def test_cooldown_respected(session):
    """Borrower with future next_eligible_at must be skipped."""
    make_agent(session)
    future = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)  # 1h after NOW
    make_borrower(session, next_eligible_at=future)
    make_borrower(session, next_eligible_at=None)  # eligible immediately
    session.commit()

    with transaction(session):
        call_ids = reserve_for_progressive(session, CAMPAIGN_ID, max_n=5, now=NOW)

    assert len(call_ids) == 1  # only the immediately-eligible borrower


# ------------------------------------------------------------------ #
# Race tests                                                          #
# ------------------------------------------------------------------ #

def _race_worker(session_factory, campaign_id, results: list, barrier: threading.Barrier):
    """
    Worker function: each thread waits at the barrier then tries to allocate.
    Results are appended to the shared `results` list.
    Thread safety of list.append() for simple objects is guaranteed in CPython.
    """
    session = session_factory()
    try:
        barrier.wait()  # all threads start simultaneously
        with transaction(session):
            ids = reserve_for_progressive(session, campaign_id, max_n=50, now=NOW)
        results.append(ids)
    except Exception as e:
        results.append([])  # failed worker contributes nothing
    finally:
        session.close()


def _run_race_test(db_engine, n_threads: int, n_agents: int, n_borrowers: int):
    """Set up agents/borrowers, run n_threads concurrently, check invariants."""
    sf = make_session_factory(db_engine)
    setup_session = sf()
    try:
        for _ in range(n_agents):
            make_agent(setup_session)
        for _ in range(n_borrowers):
            make_borrower(setup_session)
        setup_session.commit()
    finally:
        setup_session.close()

    barrier = threading.Barrier(n_threads)
    results: list[list[uuid.UUID]] = []

    threads = [
        threading.Thread(target=_race_worker, args=(sf, CAMPAIGN_ID, results, barrier))
        for _ in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # Flatten all allocated call IDs across all workers.
    all_call_ids = [cid for worker_ids in results for cid in worker_ids]

    # Verify via DB queries (use a fresh session).
    verify_session = sf()
    try:
        # Total reserved == min(n_agents, n_borrowers)
        expected_total = min(n_agents, n_borrowers)
        assert len(all_call_ids) == expected_total, (
            f"Expected {expected_total} calls, got {len(all_call_ids)}"
        )

        # No duplicate call IDs
        assert len(set(all_call_ids)) == len(all_call_ids), "Duplicate call IDs!"

        # No agent with 2 active calls
        rows = verify_session.execute(
            text("""
                SELECT agent_id, COUNT(*) as cnt
                FROM calls
                WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED')
                  AND agent_id IS NOT NULL
                GROUP BY agent_id
                HAVING COUNT(*) > 1
            """)
        ).fetchall()
        assert rows == [], f"Agent(s) with >1 active call: {rows}"

        # No borrower with 2 active calls
        rows = verify_session.execute(
            text("""
                SELECT borrower_id, COUNT(*) as cnt
                FROM calls
                WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED')
                GROUP BY borrower_id
                HAVING COUNT(*) > 1
            """)
        ).fetchall()
        assert rows == [], f"Borrower(s) with >1 active call: {rows}"

        # Every reserved agent has exactly one matching call
        reserved_agents = verify_session.execute(
            text("SELECT id FROM agents WHERE state = 'RESERVED'")
        ).scalars().all()
        assert len(reserved_agents) == expected_total

        for agent_id in reserved_agents:
            call_count = verify_session.execute(
                text("""
                    SELECT COUNT(*) FROM calls
                    WHERE agent_id = :aid
                      AND state NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED')
                """),
                {"aid": agent_id},
            ).scalar()
            assert call_count == 1, (
                f"Agent {agent_id} has {call_count} active calls (expected 1)"
            )
    finally:
        verify_session.close()


def test_race_2_threads(db_engine):
    """2 threads compete for 5 agents / 50 borrowers."""
    _run_race_test(db_engine, n_threads=2, n_agents=5, n_borrowers=50)


def test_race_8_threads(db_engine):
    """8 threads compete for 5 agents / 50 borrowers."""
    _run_race_test(db_engine, n_threads=8, n_agents=5, n_borrowers=50)


# ------------------------------------------------------------------ #
# Constraint backstop test                                            #
# ------------------------------------------------------------------ #

def test_constraint_backstop(session, db_engine):
    """
    Bypass the allocator entirely and try to insert two active call rows
    for the same agent via raw SQL.  The partial unique index must reject it.
    """
    sf = make_session_factory(db_engine)
    s = sf()
    try:
        agent = make_agent(s, state="RESERVED")
        borrower1 = make_borrower(s)
        borrower2 = make_borrower(s)
        s.commit()

        # First call row (valid)
        call_id_1 = uuid.uuid4()
        s.execute(
            text("""
                INSERT INTO calls
                  (id, campaign_id, borrower_id, agent_id, provider,
                   state, state_rank, version, created_at, updated_at)
                VALUES
                  (:id, :cid, :bid, :aid, 'mock',
                   'RESERVED', 1, 0, NOW(), NOW())
            """),
            {"id": call_id_1, "cid": CAMPAIGN_ID, "bid": borrower1.id, "aid": agent.id},
        )
        s.commit()

        # Second call row for the same agent — must violate the partial unique index.
        call_id_2 = uuid.uuid4()
        with pytest.raises(IntegrityError):
            s.execute(
                text("""
                    INSERT INTO calls
                      (id, campaign_id, borrower_id, agent_id, provider,
                       state, state_rank, version, created_at, updated_at)
                    VALUES
                      (:id, :cid, :bid, :aid, 'mock',
                       'RESERVED', 1, 0, NOW(), NOW())
                """),
                {"id": call_id_2, "cid": CAMPAIGN_ID, "bid": borrower2.id, "aid": agent.id},
            )
            s.commit()
    finally:
        s.rollback()
        s.close()


# ------------------------------------------------------------------ #
# Rollback / no-leak test                                             #
# ------------------------------------------------------------------ #

def test_rollback_no_leak(db_engine):
    """
    Simulate an exception AFTER agents/borrowers are updated to RESERVED
    but BEFORE the transaction commits.  Verify zero leaked RESERVED rows.

    Implementation: we patch session.flush() to raise after the first call
    so the agents/borrowers have been dirtied but the transaction rolls back.
    """
    sf = make_session_factory(db_engine)
    setup = sf()
    try:
        for _ in range(3):
            make_agent(setup)
        for _ in range(5):
            make_borrower(setup)
        setup.commit()
    finally:
        setup.close()

    # Now run the allocator but roll back the transaction.
    s = sf()
    try:
        with pytest.raises(RuntimeError, match="simulated mid-tx failure"):
            with transaction(s):
                # Dirty the rows
                ids = reserve_for_progressive(s, CAMPAIGN_ID, max_n=3, now=NOW)
                assert len(ids) == 3  # rows dirtied in session
                # Now raise — the transaction context manager will rollback
                raise RuntimeError("simulated mid-tx failure")
    finally:
        s.close()

    # Verify: no RESERVED agents or borrowers remain.
    verify = sf()
    try:
        reserved_agents = verify.execute(
            text("SELECT COUNT(*) FROM agents WHERE state='RESERVED'")
        ).scalar()
        reserved_borrowers = verify.execute(
            text("SELECT COUNT(*) FROM borrowers WHERE state='RESERVED'")
        ).scalar()
        call_count = verify.execute(
            text("SELECT COUNT(*) FROM calls")
        ).scalar()

        assert reserved_agents == 0, f"Leaked {reserved_agents} RESERVED agent(s)"
        assert reserved_borrowers == 0, f"Leaked {reserved_borrowers} RESERVED borrower(s)"
        assert call_count == 0, f"Leaked {call_count} call row(s)"
    finally:
        verify.close()
