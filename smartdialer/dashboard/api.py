"""
smartdialer.dashboard.api — FastAPI live operator dashboard and control console.

Runs read endpoints inside strictly read-only transactions (SET TRANSACTION READ ONLY),
never takes row locks, and never exposes phone_tokens or contact information.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from smartdialer.config import settings
from smartdialer.db.models import (
    Agent,
    Borrower,
    Call,
    CallEvent,
    MetricsSnapshot,
    OperatorActionLog,
    PacingDecisionLog,
    SafetyDecisionLog,
    SystemFlag,
)
from smartdialer.db.session import get_session_factory, transaction
from smartdialer.domain.agent_fsm import AgentState, apply_transition
from smartdialer.domain.call_fsm import CallState
from smartdialer.providers.registry import get as get_provider
from smartdialer.safety.controller import set_kill_switch

STATIC_DIR = Path(__file__).resolve().parent / "static"
OPERATOR_TOKEN_ENV = os.environ.get("OPERATOR_TOKEN", "secret-operator-token")

app = FastAPI(
    title="SmartDialer Live Operator Dashboard",
    description="Real-time monitoring, algorithmic explainability, and operator control console.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global simulation hook (for live demo controls)
_live_demo_controls: dict[str, Any] = {
    "sim_answer_rate": None,
    "last_sweeper_summary": {},
}


# --------------------------------------------------------------------------- #
# Security & Read-Only DB Dependencies                                        #
# --------------------------------------------------------------------------- #

def get_readonly_session() -> Session:
    """Provides a Session configured with SET TRANSACTION READ ONLY."""
    session_factory = get_session_factory()
    session = session_factory()
    session.execute(text("SET TRANSACTION READ ONLY"))
    try:
        yield session
    finally:
        session.close()


def verify_operator_token(
    x_operator_token: str | None = Header(None, alias="X-Operator-Token")
) -> str:
    """Guards POST control endpoints against unauthorized access."""
    if not x_operator_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Operator-Token header",
        )
    if x_operator_token != OPERATOR_TOKEN_ENV:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid operator token",
        )
    return x_operator_token


def log_operator_action(session: Session, action: str, payload: dict[str, Any], token: str) -> None:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    audit_row = OperatorActionLog(
        action=action,
        payload=payload,
        operator_token_hash=token_hash,
        created_at=datetime.now(timezone.utc),
    )
    session.add(audit_row)
    session.commit()


# --------------------------------------------------------------------------- #
# DTO Models                                                                  #
# --------------------------------------------------------------------------- #

class KillSwitchRequest(BaseModel):
    on: bool

class ProviderOutageRequest(BaseModel):
    provider: str
    on: bool

class DropAgentsRequest(BaseModel):
    count: int = Field(ge=1, le=1000)

class AnswerRateRequest(BaseModel):
    value: float = Field(ge=0.01, le=1.0)

class InjectEventsRequest(BaseModel):
    kind: str = Field(pattern="^(duplicate|out_of_order)$")


# --------------------------------------------------------------------------- #
# Read Endpoints (GET) — Pure Read-Only, Non-Locking                          #
# --------------------------------------------------------------------------- #

@app.get("/api/overview")
def get_overview(
    campaign_id: int = Query(1),
    session: Session = Depends(get_readonly_session),
) -> dict[str, Any]:
    # 1. Agent counts by state
    agent_counts_stmt = (
        select(Agent.state, func.count(Agent.id))
        .where(Agent.campaign_id == campaign_id)
        .group_by(Agent.state)
    )
    agent_counts = dict(session.execute(agent_counts_stmt).all())
    for st in [s.value for s in AgentState]:
        agent_counts.setdefault(st, 0)

    online_agents = sum(
        agent_counts[s]
        for s in [
            AgentState.AVAILABLE.value,
            AgentState.RESERVED.value,
            AgentState.DIALING.value,
            AgentState.CONNECTED.value,
            AgentState.WRAP_UP.value,
            AgentState.PAUSED.value,
        ]
    )
    busy_agents = (
        agent_counts[AgentState.CONNECTED.value]
        + agent_counts[AgentState.DIALING.value]
        + agent_counts[AgentState.RESERVED.value]
    )
    utilization_pct = (busy_agents / max(1, online_agents)) * 100.0

    # 2. Call counts by state
    call_counts_stmt = (
        select(Call.state, func.count(Call.id))
        .where(Call.campaign_id == campaign_id)
        .group_by(Call.state)
    )
    call_counts = dict(session.execute(call_counts_stmt).all())
    in_flight = call_counts.get("INITIATED", 0) + call_counts.get("RINGING", 0)
    held = call_counts.get("HELD", 0)
    connected = call_counts.get("CONNECTED", 0)
    completed = call_counts.get("COMPLETED", 0)
    abandoned = call_counts.get("ABANDONED", 0)

    # 3. Rolling abandonment rate
    eligible_abandon_total = connected + completed + abandoned
    rolling_abandon_rate = (abandoned / max(1, eligible_abandon_total))

    # 4. Kill switch flag
    kill_switch_row = session.execute(
        select(SystemFlag.flag_value).where(SystemFlag.flag_name == "kill_switch")
    ).scalar_one_or_none()
    kill_switch_active = (kill_switch_row is not None and kill_switch_row.strip().lower() in ("true", "1", "yes", "on"))

    # 5. Latest safety decision to determine current operating mode
    latest_safety = session.execute(
        select(SafetyDecisionLog.action)
        .where(SafetyDecisionLog.campaign_id == campaign_id)
        .order_by(SafetyDecisionLog.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if kill_switch_active:
        campaign_mode = "KILL_SWITCH"
    elif latest_safety == "FALLBACK_PROGRESSIVE":
        campaign_mode = "FALLBACK_PROGRESSIVE"
    elif latest_safety in ("APPROVE", "REDUCE"):
        campaign_mode = "PREDICTIVE"
    else:
        campaign_mode = "PROGRESSIVE"

    return {
        "campaign_id": campaign_id,
        "mode": campaign_mode,
        "kill_switch_active": kill_switch_active,
        "agents": agent_counts,
        "online_agents": online_agents,
        "busy_agents": busy_agents,
        "utilization_pct": round(utilization_pct, 1),
        "in_flight": in_flight,
        "held": held,
        "connected": connected,
        "completed": completed,
        "abandoned": abandoned,
        "rolling_abandonment_rate": round(rolling_abandon_rate, 4),
        "abandonment_budget": settings.abandonment_budget,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/timeseries")
def get_timeseries(
    campaign_id: int = Query(1),
    window: int = Query(300),
    session: Session = Depends(get_readonly_session),
) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window)
    stmt = (
        select(MetricsSnapshot)
        .where(
            MetricsSnapshot.campaign_id == campaign_id,
            MetricsSnapshot.created_at >= cutoff,
        )
        .order_by(MetricsSnapshot.created_at.asc())
        .limit(300)
    )
    rows = session.scalars(stmt).all()
    return [
        {
            "time": r.created_at.isoformat(),
            "in_flight": r.in_flight,
            "capacity": r.capacity,
            "connected": r.connected,
            "abandoned": r.abandoned,
            "utilization_pct": float(r.utilization_pct),
            "p_mean": float(r.p_mean),
            "p_hi": float(r.p_hi),
            "sim_answer_rate": _live_demo_controls["sim_answer_rate"],
        }
        for r in rows
    ]


@app.get("/api/pacing/decisions")
def get_pacing_decisions(
    campaign_id: int = Query(1),
    limit: int = Query(20, le=100),
    session: Session = Depends(get_readonly_session),
) -> list[dict[str, Any]]:
    stmt = (
        select(PacingDecisionLog)
        .where(PacingDecisionLog.campaign_id == campaign_id)
        .order_by(PacingDecisionLog.created_at.desc())
        .limit(limit)
    )
    rows = session.scalars(stmt).all()
    return [
        {
            "id": r.id,
            "requested_calls": r.requested_calls,
            "capacity": r.capacity,
            "inflight": r.inflight,
            "p_hi": float(r.p_hi),
            "max_calls": r.max_calls,
            "formula": r.formula,
            "why": r.reason,
            "inputs": r.inputs,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/api/safety/decisions")
def get_safety_decisions(
    campaign_id: int = Query(1),
    limit: int = Query(20, le=100),
    session: Session = Depends(get_readonly_session),
) -> list[dict[str, Any]]:
    stmt = (
        select(SafetyDecisionLog)
        .where(SafetyDecisionLog.campaign_id == campaign_id)
        .order_by(SafetyDecisionLog.created_at.desc())
        .limit(limit)
    )
    rows = session.scalars(stmt).all()
    return [
        {
            "id": r.id,
            "action": r.action,
            "approved_calls": r.approved_calls,
            "rule_ids": r.rule_ids,
            "reasons": r.reasons,
            "inputs": r.inputs,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/api/providers")
def get_providers_health() -> dict[str, Any]:
    results = {}
    for name in ["mock_a", "mock_b"]:
        try:
            prov = get_provider(name)
            if hasattr(prov, "health_snapshot"):
                results[name] = prov.health_snapshot()
            else:
                results[name] = {"circuit_state": "CLOSED", "failure_rate": 0.0, "latency_p50_ms": 50.0}
        except Exception:
            results[name] = {"circuit_state": "UNKNOWN", "failure_rate": 1.0, "latency_p50_ms": 0.0}
    return results


@app.get("/api/events")
def get_events_breakdown(session: Session = Depends(get_readonly_session)) -> dict[str, Any]:
    counts_stmt = select(CallEvent.status, func.count(CallEvent.id)).group_by(CallEvent.status)
    status_counts = dict(session.execute(counts_stmt).all())

    late_answers_stmt = select(func.count(CallEvent.id)).where(
        CallEvent.status == "IGNORED_LATE",
        CallEvent.event_type == "ANSWERED",
    )
    late_answers = session.scalar(late_answers_stmt) or 0

    return {
        "status_counts": {
            "APPLIED": status_counts.get("APPLIED", 0),
            "IGNORED_DUPLICATE": status_counts.get("IGNORED_DUPLICATE", 0),
            "IGNORED_LATE": status_counts.get("IGNORED_LATE", 0),
            "QUARANTINED": status_counts.get("QUARANTINED", 0),
            "DEAD_LETTER": status_counts.get("DEAD_LETTER", 0),
        },
        "late_answer_after_cancel": late_answers,
    }


@app.get("/api/recovery")
def get_recovery_status() -> dict[str, Any]:
    return {
        "sweeper_active": True,
        "last_summary": _live_demo_controls["last_sweeper_summary"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/invariants")
def get_invariants_status(
    campaign_id: int = Query(1),
    session: Session = Depends(get_readonly_session),
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)

    # 1. Agents with > 1 active call
    active_agent_stmt = (
        select(Call.agent_id, func.count(Call.id))
        .where(
            Call.agent_id.is_not(None),
            Call.state.not_in(["COMPLETED", "FAILED", "CANCELLED", "ABANDONED"]),
        )
        .group_by(Call.agent_id)
        .having(func.count(Call.id) > 1)
    )
    double_agents = session.execute(active_agent_stmt).all()

    # 2. Borrowers with > 1 active call
    active_borrower_stmt = (
        select(Call.borrower_id, func.count(Call.id))
        .where(Call.state.not_in(["COMPLETED", "FAILED", "CANCELLED", "ABANDONED"]))
        .group_by(Call.borrower_id)
        .having(func.count(Call.id) > 1)
    )
    double_borrowers = session.execute(active_borrower_stmt).all()

    # 3. Stale HELD calls (> hold_timeout_seconds)
    stale_held_stmt = (
        select(func.count(Call.id))
        .where(
            Call.campaign_id == campaign_id,
            Call.state == CallState.HELD.value,
            Call.held_since < (now - timedelta(seconds=settings.hold_timeout_seconds + 1.0)),
        )
    )
    stale_held_count = session.execute(stale_held_stmt).scalar() or 0

    all_passed = (len(double_agents) == 0 and len(double_borrowers) == 0 and stale_held_count == 0)

    return {
        "status": "PASSED" if all_passed else "FAILED",
        "double_agent_count": len(double_agents),
        "double_borrower_count": len(double_borrowers),
        "stale_held_count": stale_held_count,
        "timestamp": now.isoformat(),
    }


# --------------------------------------------------------------------------- #
# Operator Control Endpoints (POST) — Audited & Token Protected               #
# --------------------------------------------------------------------------- #

@app.post("/api/control/kill-switch")
def post_kill_switch(
    body: KillSwitchRequest,
    token: str = Depends(verify_operator_token),
) -> dict[str, Any]:
    session_factory = get_session_factory()
    session = session_factory()
    try:
        set_kill_switch(session, active=body.on)
        log_operator_action(session, "KILL_SWITCH", {"on": body.on}, token)
        return {"status": "SUCCESS", "kill_switch_active": body.on}
    finally:
        session.close()


@app.post("/api/control/provider-outage")
def post_provider_outage(
    body: ProviderOutageRequest,
    token: str = Depends(verify_operator_token),
) -> dict[str, Any]:
    try:
        prov = get_provider(body.provider)
        if hasattr(prov, "set_outage"):
            prov.set_outage(body.on)
        else:
            raise HTTPException(status_code=400, detail="Provider does not support failure injection")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Provider '{body.provider}' not found")

    session_factory = get_session_factory()
    session = session_factory()
    try:
        log_operator_action(session, "PROVIDER_OUTAGE", {"provider": body.provider, "on": body.on}, token)
    finally:
        session.close()

    return {"status": "SUCCESS", "provider": body.provider, "outage": body.on}


@app.post("/api/control/drop-agents")
def post_drop_agents(
    body: DropAgentsRequest,
    token: str = Depends(verify_operator_token),
) -> dict[str, Any]:
    session_factory = get_session_factory()
    session = session_factory()
    dropped = 0
    now = datetime.now(timezone.utc)
    try:
        with transaction(session):
            agents = session.scalars(
                select(Agent)
                .where(Agent.state == AgentState.AVAILABLE.value)
                .limit(body.count)
                .with_for_update(skip_locked=True)
            ).all()

            for ag in agents:
                # Transition via agent FSM to preserve invariants
                new_state, ok, _ = apply_transition(AgentState(ag.state), AgentState.OFFLINE)
                if ok:
                    ag.state = new_state.value
                    ag.last_state_change_at = now
                    ag.updated_at = now
                    ag.version += 1
                    dropped += 1

        log_operator_action(session, "DROP_AGENTS", {"requested": body.count, "dropped": dropped}, token)
        return {"status": "SUCCESS", "requested": body.count, "dropped": dropped}
    finally:
        session.close()


@app.post("/api/control/answer-rate")
def post_answer_rate(
    body: AnswerRateRequest,
    token: str = Depends(verify_operator_token),
) -> dict[str, Any]:
    _live_demo_controls["sim_answer_rate"] = body.value
    session_factory = get_session_factory()
    session = session_factory()
    try:
        log_operator_action(session, "SET_SIM_ANSWER_RATE", {"value": body.value}, token)
    finally:
        session.close()

    return {"status": "SUCCESS", "sim_answer_rate": body.value}


@app.post("/api/control/inject-events")
def post_inject_events(
    body: InjectEventsRequest,
    token: str = Depends(verify_operator_token),
) -> dict[str, Any]:
    session_factory = get_session_factory()
    session = session_factory()
    now = datetime.now(timezone.utc)
    try:
        # Find an active call to inject against
        call = session.scalars(
            select(Call).where(Call.state.in_(["INITIATED", "RINGING", "ANSWERED"])).limit(1)
        ).first()

        if not call:
            return {"status": "SKIPPED", "detail": "No active in-flight call found to storm"}

        events_injected = 0
        if body.kind == "duplicate":
            # Ingest 5 duplicate ANSWERED events with the same event_id
            ev_id = f"storm_dup_{call.id}"
            for _ in range(5):
                session.execute(
                    text(
                        "INSERT INTO call_events (provider, event_id, call_id, provider_call_id, event_type, payload, status, attempts, received_at) "
                        "VALUES (:provider, :event_id, :call_id, :provider_call_id, 'ANSWERED', '{}', 'RECEIVED', 0, :now) "
                        "ON CONFLICT (provider, event_id) DO NOTHING"
                    ),
                    {"provider": call.provider, "event_id": ev_id, "call_id": call.id, "provider_call_id": call.provider_call_id, "now": now},
                )
                events_injected += 1
            session.commit()
        elif body.kind == "out_of_order":
            # Ingest COMPLETED before ANSWERED
            session.execute(
                text(
                    "INSERT INTO call_events (provider, event_id, call_id, provider_call_id, event_type, payload, status, attempts, received_at) "
                    "VALUES (:provider, :ev1, :call_id, :provider_call_id, 'COMPLETED', '{}', 'RECEIVED', 0, :now) "
                    "ON CONFLICT (provider, event_id) DO NOTHING"
                ),
                {"provider": call.provider, "ev1": f"storm_comp_{call.id}", "call_id": call.id, "provider_call_id": call.provider_call_id, "now": now},
            )
            session.execute(
                text(
                    "INSERT INTO call_events (provider, event_id, call_id, provider_call_id, event_type, payload, status, attempts, received_at) "
                    "VALUES (:provider, :ev2, :call_id, :provider_call_id, 'ANSWERED', '{}', 'RECEIVED', 0, :now) "
                    "ON CONFLICT (provider, event_id) DO NOTHING"
                ),
                {"provider": call.provider, "ev2": f"storm_ans_{call.id}", "call_id": call.id, "provider_call_id": call.provider_call_id, "now": now},
            )
            session.commit()
            events_injected = 2

        log_operator_action(session, "INJECT_EVENTS", {"kind": body.kind, "injected": events_injected}, token)
        return {"status": "SUCCESS", "kind": body.kind, "injected": events_injected}
    finally:
        session.close()


# --------------------------------------------------------------------------- #
# Static Frontend Serving                                                     #
# --------------------------------------------------------------------------- #

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/", include_in_schema=False)
def get_root():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return JSONResponse({"status": "Dashboard frontend index.html not yet generated"})
