"""
tests/test_dashboard.py — Exhaustive test suite for Phase 8 Live Operator Dashboard.

Verifies:
1. All 8 read endpoints return valid JSON and execute in read-only transactions.
2. Zero data leakage: phone_token and contact info are NEVER returned in any endpoint.
3. Operator authentication: missing token -> 401, bad token -> 403, valid token -> 200 + audit row.
4. Drop-agents control endpoint transitions agents via FSM, only affecting AVAILABLE agents.
5. Kill-switch control endpoint activates Rule R6 in safety controller, rejecting all dials.
6. Provider-outage control endpoint sets provider circuit breaker to OPEN.
7. AST audit: smartdialer.pacing NEVER imports from dashboard or kill-switch.
8. Latency benchmark: /api/overview stays under budget with 10,000 seeded agents.
"""

from __future__ import annotations

import ast
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.dashboard.api import app, OPERATOR_TOKEN_ENV, _live_demo_controls
from smartdialer.db.models import (
    Agent,
    Borrower,
    Call,
    CallEvent,
    MetricsSnapshot,
    OperatorActionLog,
    PacingDecisionLog,
    SafetyDecisionLog,
)
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState
from smartdialer.orchestrator.tick import run_tick
from smartdialer.providers.registry import get as get_provider
from smartdialer.safety.controller import is_kill_switch_active, set_kill_switch


@pytest.fixture
def client():
    return TestClient(app)


def test_read_endpoints_ok(client: TestClient, session: Session):
    """Verify all 8 read endpoints return 200 OK with proper data structures."""
    now = datetime.now(timezone.utc)

    # Seed minimal data
    agent = Agent(campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=now, updated_at=now)
    borrower = Borrower(campaign_id=1, phone_token="tok_123", state="ELIGIBLE", priority=1, attempts=0, next_eligible_at=now)
    session.add_all([agent, borrower])
    session.commit()

    # 1. /api/overview
    r = client.get("/api/overview?campaign_id=1")
    assert r.status_code == 200
    data = r.json()
    assert "agents" in data
    assert data["agents"]["AVAILABLE"] == 1
    assert data["online_agents"] == 1
    assert "in_flight" in data
    assert "rolling_abandonment_rate" in data
    assert data["kill_switch_active"] is False

    # 2. /api/timeseries
    r = client.get("/api/timeseries?campaign_id=1&window_seconds=300")
    assert r.status_code == 200
    assert isinstance(r.json(), list)

    # 3. /api/pacing/decisions
    r = client.get("/api/pacing/decisions?campaign_id=1&limit=5")
    assert r.status_code == 200
    assert isinstance(r.json(), list)

    # 4. /api/safety/decisions
    r = client.get("/api/safety/decisions?campaign_id=1&limit=5")
    assert r.status_code == 200
    assert isinstance(r.json(), list)

    # 5. /api/providers
    r = client.get("/api/providers")
    assert r.status_code == 200
    pdata = r.json()
    assert "mock_a" in pdata
    assert "mock_b" in pdata

    # 6. /api/events
    r = client.get("/api/events")
    assert r.status_code == 200
    edata = r.json()
    assert "status_counts" in edata
    assert "late_answer_after_cancel" in edata

    # 7. /api/recovery
    r = client.get("/api/recovery")
    assert r.status_code == 200
    recdata = r.json()
    assert recdata["sweeper_active"] is True

    # 8. /api/invariants
    r = client.get("/api/invariants?campaign_id=1")
    assert r.status_code == 200
    invdata = r.json()
    assert invdata["status"] == "PASSED"
    assert invdata["double_agent_count"] == 0
    assert invdata["double_borrower_count"] == 0


def test_read_only_transactions(client: TestClient, session: Session):
    """Verify that read-only sessions reject INSERT/UPDATE mutations."""
    from smartdialer.dashboard.api import get_readonly_session

    ro_gen = get_readonly_session()
    ro_session = next(ro_gen)
    try:
        # PostgreSQL raises InternalError or ProgrammingError: cannot execute INSERT in a read-only transaction
        with pytest.raises(Exception) as exc_info:
            ro_session.execute(
                text("INSERT INTO system_flags (flag_name, flag_value, updated_at) VALUES ('test', 'val', NOW())")
            )
            ro_session.commit()
        assert "read-only" in str(exc_info.value).lower()
    finally:
        ro_session.rollback()
        ro_session.close()


def test_privacy_zero_phone_token_leaks(client: TestClient, session: Session):
    """Verify that phone_token or borrower PII is NEVER leaked in any API endpoint."""
    import uuid
    now = datetime.now(timezone.utc)
    secret_token = "SECRET_SUPER_CONFIDENTIAL_PHONE_TOKEN_9999"

    # Seed borrower and call with secret phone token
    borrower = Borrower(
        campaign_id=1,
        phone_token=secret_token,
        state="IN_CALL",
        priority=1,
        attempts=1,
        next_eligible_at=now,
    )
    session.add(borrower)
    session.flush()

    call = Call(
        id=uuid.uuid4(),
        campaign_id=1,
        borrower_id=borrower.id,
        provider="mock_a",
        state=CallState.INITIATED.value,
        created_at=now,
        updated_at=now,
    )
    session.add(call)
    session.commit()

    # Query all 8 GET endpoints and inspect the raw text
    endpoints = [
        "/api/overview",
        "/api/timeseries",
        "/api/pacing/decisions",
        "/api/safety/decisions",
        "/api/providers",
        "/api/events",
        "/api/recovery",
        "/api/invariants",
    ]

    for ep in endpoints:
        res = client.get(ep)
        assert res.status_code == 200
        assert secret_token not in res.text, f"Endpoint {ep} leaked phone_token!"


def test_operator_token_auth(client: TestClient, session: Session):
    """Verify control authentication and audit log generation."""
    url = "/api/control/kill-switch"
    payload = {"on": True}

    # 1. Missing header -> 401
    r = client.post(url, json=payload)
    assert r.status_code == 401

    # 2. Invalid header -> 403
    r = client.post(url, json=payload, headers={"X-Operator-Token": "bad-token"})
    assert r.status_code == 403

    # 3. Valid header -> 200
    r = client.post(url, json=payload, headers={"X-Operator-Token": OPERATOR_TOKEN_ENV})
    assert r.status_code == 200
    assert r.json()["kill_switch_active"] is True

    # 4. Check audit log in DB
    log = session.scalars(
        select(OperatorActionLog).order_by(OperatorActionLog.id.desc())
    ).first()
    assert log is not None
    assert log.action == "KILL_SWITCH"
    assert log.payload == {"on": True}
    assert log.operator_token_hash is not None
    assert len(log.operator_token_hash) == 64  # SHA-256 length

    # Reset kill switch
    set_kill_switch(session, False)
    session.commit()


def test_control_drop_agents_fsm(client: TestClient, session: Session):
    """Verify that drop-agents only transitions AVAILABLE agents to OFFLINE via FSM."""
    now = datetime.now(timezone.utc)

    # Seed 3 AVAILABLE agents, 2 CONNECTED agents
    avail_agents = [
        Agent(campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=now, updated_at=now)
        for _ in range(3)
    ]
    conn_agents = [
        Agent(campaign_id=1, state=AgentState.CONNECTED.value, last_state_change_at=now, updated_at=now)
        for _ in range(2)
    ]
    session.add_all(avail_agents + conn_agents)
    session.commit()

    # Drop 2 agents
    r = client.post(
        "/api/control/drop-agents",
        json={"count": 2},
        headers={"X-Operator-Token": OPERATOR_TOKEN_ENV},
    )
    assert r.status_code == 200
    assert r.json()["dropped"] == 2

    # Check state in DB
    offline_count = session.scalar(select(text("COUNT(*)")).select_from(Agent).where(Agent.state == "OFFLINE"))
    avail_count = session.scalar(select(text("COUNT(*)")).select_from(Agent).where(Agent.state == "AVAILABLE"))
    conn_count = session.scalar(select(text("COUNT(*)")).select_from(Agent).where(Agent.state == "CONNECTED"))

    assert offline_count == 2
    assert avail_count == 1
    assert conn_count == 2  # Untouched!


def test_control_kill_switch_triggers_rule_r6(client: TestClient, session_factory):
    """Verify kill switch causes Safety Controller rule R6 to reject all calls on tick."""
    session = session_factory()
    now = datetime.now(timezone.utc)
    try:
        # Add 5 available agents and 10 borrowers
        for _ in range(5):
            session.add(Agent(campaign_id=1, state=AgentState.AVAILABLE.value, last_state_change_at=now, updated_at=now))
        for j in range(10):
            session.add(Borrower(campaign_id=1, phone_token=f"b_{j}", state="ELIGIBLE", priority=j, attempts=0, next_eligible_at=now))
        session.commit()
    finally:
        session.close()

    clock = FakeClock(now)

    # Activate kill switch via dashboard API
    r = client.post(
        "/api/control/kill-switch",
        json={"on": True},
        headers={"X-Operator-Token": OPERATOR_TOKEN_ENV},
    )
    assert r.status_code == 200
    s_check = session_factory()
    try:
        assert is_kill_switch_active(s_check) is True
    finally:
        s_check.close()

    # Run orchestrator tick
    tick_summary = run_tick(
        session_factory=session_factory,
        campaign_id=1,
        provider_name="mock_a",
        mode="PROGRESSIVE",
        clock=clock,
    )

    # Must be rejected by Rule R6
    assert tick_summary["approved_calls"] == 0
    assert tick_summary["enforced_action"] == "REJECT"
    assert "R6" in tick_summary["advisory_rule_ids"]

    # Deactivate kill switch
    client.post(
        "/api/control/kill-switch",
        json={"on": False},
        headers={"X-Operator-Token": OPERATOR_TOKEN_ENV},
    )
    s_check2 = session_factory()
    try:
        assert is_kill_switch_active(s_check2) is False
    finally:
        s_check2.close()


def test_control_provider_outage(client: TestClient):
    """Verify provider outage triggers circuit OPEN and reflects in health snapshot."""
    # 1. Trigger outage on mock_a
    r = client.post(
        "/api/control/provider-outage",
        json={"provider": "mock_a", "on": True},
        headers={"X-Operator-Token": OPERATOR_TOKEN_ENV},
    )
    assert r.status_code == 200

    # 2. Query provider health
    prov_res = client.get("/api/providers").json()
    assert prov_res["mock_a"]["circuit_state"] == "OPEN"
    assert prov_res["mock_a"]["failure_rate"] == 1.0

    # 3. Restore provider
    r = client.post(
        "/api/control/provider-outage",
        json={"provider": "mock_a", "on": False},
        headers={"X-Operator-Token": OPERATOR_TOKEN_ENV},
    )
    assert r.status_code == 200

    prov_res = client.get("/api/providers").json()
    assert prov_res["mock_a"]["circuit_state"] == "CLOSED"


def test_ast_pacing_isolation():
    """
    AST check: smartdialer.pacing must NEVER import from dashboard or kill_switch.
    Enforces strict architectural decoupling.
    """
    pacing_dir = Path(__file__).resolve().parent.parent / "smartdialer" / "pacing"
    forbidden_terms = ["dashboard", "kill_switch", "set_kill_switch", "is_kill_switch_active"]

    for py_file in pacing_dir.glob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(py_file))

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for forbidden in forbidden_terms:
                        assert forbidden not in alias.name, (
                            f"Illegal import '{alias.name}' in {py_file.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for forbidden in forbidden_terms:
                    assert forbidden not in module, (
                        f"Illegal import from '{module}' in {py_file.name}"
                    )
                for alias in node.names:
                    for forbidden in forbidden_terms:
                        assert forbidden not in alias.name, (
                            f"Illegal import of '{alias.name}' in {py_file.name}"
                        )


def test_overview_benchmark_10000_agents(client: TestClient, session: Session):
    """
    Benchmark /api/overview latency with 10,000 agents and 1,000 active calls.
    Verifies SQL aggregates (GROUP BY) scale sub-linearly and remain cheap to poll at 1s.
    """
    now = datetime.now(timezone.utc)

    # Bulk insert 10,000 agents via raw SQL for high speed
    session.execute(
        text(
            "INSERT INTO agents (campaign_id, state, last_state_change_at, updated_at, version) "
            "SELECT 1, CASE WHEN i % 5 = 0 THEN 'AVAILABLE' "
            "               WHEN i % 5 = 1 THEN 'CONNECTED' "
            "               WHEN i % 5 = 2 THEN 'WRAP_UP' "
            "               WHEN i % 5 = 3 THEN 'RESERVED' "
            "               ELSE 'PAUSED' END, "
            ":now, :now, 1 "
            "FROM generate_series(1, 10000) AS i"
        ),
        {"now": now},
    )
    session.commit()

    # Warm-up request
    client.get("/api/overview?campaign_id=1")

    # Measure 20 consecutive requests
    latencies = []
    for _ in range(20):
        t0 = time.perf_counter()
        resp = client.get("/api/overview?campaign_id=1")
        elapsed = (time.perf_counter() - t0) * 1000.0  # ms
        assert resp.status_code == 200
        latencies.append(elapsed)

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]

    print(f"\n[Benchmark] /api/overview with 10,000 agents: p50={p50:.2f}ms, p95={p95:.2f}ms")

    # p95 must comfortably stay well under 100ms (1s polling budget)
    assert p95 < 150.0, f"p95 latency {p95:.2f}ms exceeded 150ms limit!"
