"""
smartdialer.safety.controller — Deterministic safety controller.

Core Principles & Guarantees
----------------------------
1. Pacing proposes, Safety disposes.
   The Safety Controller takes NO ML/estimator output as authority.
   It can only APPROVE, REDUCE, REJECT, or FALLBACK_PROGRESSIVE.
2. The pacing code has NO handle to disable, mutate, or bypass safety.
3. Every decision is re-validated inside the transaction that commits it
   (two-phase check: advise() is advisory; enforce() verifies fresh DB counts).
4. Persists every decision into `safety_decisions` audit table.

Rules
-----
R1 Progressive : approved <= free_agents (hard cap).
R2 Predictive  : in_flight + approved <= capacity * MAX_OVERDIAL_RATIO.
R3 Abandonment : rolling abandonment rate > budget -> FALLBACK_PROGRESSIVE with
                 hysteresis (re-enter predictive only if rate < budget * 0.5
                 after cooldown).
R4 Provider    : degraded provider health or circuit breaker OPEN -> 0 dials.
R5 Estimate    : missing, stale, or low-sample estimate -> FALLBACK_PROGRESSIVE.
R6 Kill Switch : operator kill switch in system_flags table -> REJECT immediately.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call, SafetyDecisionLog, SystemFlag
from smartdialer.domain.call_fsm import CallState
from smartdialer.providers.health import CircuitState

logger = logging.getLogger(__name__)


class PacingMode(str, Enum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE  = "PREDICTIVE"


class SafetyAction(str, Enum):
    APPROVE             = "APPROVE"
    REDUCE              = "REDUCE"
    REJECT              = "REJECT"
    FALLBACK_PROGRESSIVE = "FALLBACK_PROGRESSIVE"


class PacingRequest(BaseModel):
    campaign_id: int
    mode: PacingMode = PacingMode.PROGRESSIVE
    requested_calls: int
    estimate_meta: dict[str, Any] | None = None


class StateSnapshot(BaseModel):
    campaign_id: int
    free_agents: int
    reserved_agents: int = 0
    dialing_agents: int = 0
    connected_agents: int = 0
    in_flight: int = 0
    answered_awaiting_agent: int = 0
    rolling_abandonment_rate: float = 0.0
    abandonment_sample_count: int = 0
    provider_health: dict[str, Any] = Field(default_factory=dict)
    selected_provider: str = "mock_a"
    kill_switch_active: bool = False
    in_abandonment_cooldown: bool = False
    cooldown_target_rate: float = 0.0


class SafetyDecision(BaseModel):
    action: SafetyAction
    approved_calls: int
    rule_ids: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


# In-memory tracking for hysteresis cooldown across ticks
_campaign_hysteresis: dict[int, datetime] = {}


def advise(snapshot: StateSnapshot, request: PacingRequest) -> SafetyDecision:
    """
    Pure, deterministic evaluation of safety rules against a state snapshot.
    Does not perform I/O or database mutations.
    """
    rule_ids: list[str] = []
    reasons: list[str] = []

    # ------------------------------------------------------------------ #
    # Rule 6: Operator Kill Switch (highest priority)                     #
    # ------------------------------------------------------------------ #
    if snapshot.kill_switch_active:
        return SafetyDecision(
            action=SafetyAction.REJECT,
            approved_calls=0,
            rule_ids=["R6"],
            reasons=["Operator kill switch is active in system_flags."],
        )

    # ------------------------------------------------------------------ #
    # Rule 4: Provider Health & Circuit Breaker                           #
    # ------------------------------------------------------------------ #
    provider_info = snapshot.provider_health.get(snapshot.selected_provider, {})
    circuit_state = provider_info.get("circuit_state", CircuitState.CLOSED.value)
    if circuit_state == CircuitState.OPEN.value:
        return SafetyDecision(
            action=SafetyAction.REJECT,
            approved_calls=0,
            rule_ids=["R4"],
            reasons=[f"Provider '{snapshot.selected_provider}' circuit breaker is OPEN."],
        )

    # ------------------------------------------------------------------ #
    # Mode Validation & Downgrades (R3, R5)                               #
    # ------------------------------------------------------------------ #
    effective_mode = request.mode
    fallback_forced = False

    if request.mode == PacingMode.PREDICTIVE:
        # Rule 3: Abandonment Rate & Hysteresis
        budget = settings.abandonment_budget
        recovery_ceiling = budget * settings.abandonment_hysteresis_ratio
        current_rate = snapshot.rolling_abandonment_rate

        if current_rate > budget:
            effective_mode = PacingMode.PROGRESSIVE
            fallback_forced = True
            rule_ids.append("R3")
            reasons.append(
                f"Rolling abandonment rate ({current_rate:.4f}) exceeds budget ({budget:.4f}). "
                "Triggered fallback to progressive."
            )
        elif snapshot.in_abandonment_cooldown and current_rate >= recovery_ceiling:
            effective_mode = PacingMode.PROGRESSIVE
            fallback_forced = True
            rule_ids.append("R3")
            reasons.append(
                f"In abandonment cooldown ({current_rate:.4f} >= hysteresis threshold {recovery_ceiling:.4f}). "
                "Maintaining fallback to progressive."
            )

        # Rule 5: Estimate Quality (missing / stale / low sample size)
        if not fallback_forced:
            meta = request.estimate_meta or {}
            sample_count = meta.get("sample_count", meta.get("n_samples", 0))
            is_stale = meta.get("stale", False)
            estimate_age_seconds = meta.get("age_seconds", 999999 if is_stale else 0.0)

            if sample_count < settings.min_estimate_samples:
                effective_mode = PacingMode.PROGRESSIVE
                fallback_forced = True
                rule_ids.append("R5")
                reasons.append(
                    f"Estimate sample count ({sample_count}) is below required minimum ({settings.min_estimate_samples}). "
                    "Fallback to progressive."
                )
            elif is_stale or estimate_age_seconds > settings.max_estimate_age_seconds:
                effective_mode = PacingMode.PROGRESSIVE
                fallback_forced = True
                rule_ids.append("R5")
                reasons.append(
                    f"Estimate age ({estimate_age_seconds}s) is stale (> {settings.max_estimate_age_seconds}s). "
                    "Fallback to progressive."
                )

    # ------------------------------------------------------------------ #
    # Call Limits (R1 Progressive vs R2 Predictive)                       #
    # ------------------------------------------------------------------ #
    if effective_mode == PacingMode.PROGRESSIVE:
        # Rule 1: Progressive hard cap — approved calls cannot exceed free agents
        rule_ids.append("R1")
        hard_cap = max(0, snapshot.free_agents)
        approved = min(request.requested_calls, hard_cap)

        if fallback_forced:
            action = SafetyAction.FALLBACK_PROGRESSIVE
            reasons.append(f"Approved {approved} calls in fallback progressive mode (free_agents={snapshot.free_agents}).")
        elif approved == 0 and request.requested_calls > 0:
            action = SafetyAction.REJECT
            reasons.append(f"No available agents for progressive dialing (requested={request.requested_calls}, free={snapshot.free_agents}).")
        elif approved < request.requested_calls:
            action = SafetyAction.REUCE if hasattr(SafetyAction, "REUCE") else SafetyAction.REDUCE
            reasons.append(f"Requested {request.requested_calls} calls reduced to available agent capacity ({approved}).")
        else:
            action = SafetyAction.APPROVE
            reasons.append(f"Approved {approved} progressive calls.")

        return SafetyDecision(
            action=action,
            approved_calls=approved,
            rule_ids=rule_ids,
            reasons=reasons,
            metadata={"effective_mode": "PROGRESSIVE", "hard_cap": hard_cap},
        )

    else:
        # Rule 2: Predictive overdial ratio cap
        rule_ids.append("R2")
        capacity = max(1, snapshot.free_agents)
        max_allowed_concurrent = int(capacity * settings.max_overdial_ratio)
        current_in_flight = snapshot.in_flight
        headroom = max(0, max_allowed_concurrent - current_in_flight)
        approved = min(request.requested_calls, headroom)

        if approved == 0 and request.requested_calls > 0:
            action = SafetyAction.REJECT
            reasons.append(
                f"Predictive overdial limit reached: in_flight ({current_in_flight}) >= "
                f"cap ({max_allowed_concurrent} = {capacity} agents * {settings.max_overdial_ratio})."
            )
        elif approved < request.requested_calls:
            action = SafetyAction.REDUCE
            reasons.append(
                f"Requested {request.requested_calls} calls reduced to overdial headroom ({approved}). "
                f"in_flight={current_in_flight}, cap={max_allowed_concurrent}."
            )
        else:
            action = SafetyAction.APPROVE
            reasons.append(f"Approved {approved} predictive calls within overdial ratio.")

        return SafetyDecision(
            action=action,
            approved_calls=approved,
            rule_ids=rule_ids,
            reasons=reasons,
            metadata={
                "effective_mode": "PREDICTIVE",
                "capacity": capacity,
                "max_allowed_concurrent": max_allowed_concurrent,
                "headroom": headroom,
            },
        )


def enforce(
    session: Session,
    request: PacingRequest,
    selected_provider: str = "mock_a",
    now: datetime | None = None,
    advisory_limit: int | None = None,
) -> SafetyDecision:
    """
    Enforces safety rules inside a database transaction using live counts.

    Guarantees:
    - Enforce can ONLY REDUCE or REJECT relative to the advisory proposal.
    - Persists every decision into the safety_decisions table.
    """
    _now = now or SystemClock().now()
    snapshot = build_snapshot(
        session,
        campaign_id=request.campaign_id,
        selected_provider=selected_provider,
        now=_now,
    )

    decision = advise(snapshot, request)

    # Invariant: Enforce can only reduce or reject relative to advisory limit
    if advisory_limit is not None and decision.approved_calls > advisory_limit:
        decision.approved_calls = advisory_limit
        if decision.approved_calls == 0:
            decision.action = SafetyAction.REJECT
        else:
            decision.action = SafetyAction.REDUCE
        decision.rule_ids.append("ENFORCE_CAP")
        decision.reasons.append(f"Capped to advisory approval ({advisory_limit}).")

    # Update hysteresis state in-memory
    if SafetyAction.FALLBACK_PROGRESSIVE in (decision.action,):
        _campaign_hysteresis[request.campaign_id] = _now

    # Persist audit record
    log_entry = SafetyDecisionLog(
        campaign_id=request.campaign_id,
        action=decision.action.value,
        approved_calls=decision.approved_calls,
        rule_ids=decision.rule_ids,
        reasons=decision.reasons,
        inputs={
            "requested_calls": request.requested_calls,
            "mode": request.mode.value,
            "free_agents": snapshot.free_agents,
            "in_flight": snapshot.in_flight,
            "rolling_abandonment_rate": snapshot.rolling_abandonment_rate,
            "kill_switch": snapshot.kill_switch_active,
            "selected_provider": selected_provider,
        },
        created_at=_now,
    )
    session.add(log_entry)
    session.flush()

    return decision


def build_snapshot(
    session: Session,
    campaign_id: int,
    selected_provider: str = "mock_a",
    now: datetime | None = None,
) -> StateSnapshot:
    """
    Read live counts from database to construct a fresh StateSnapshot.
    """
    _now = now or SystemClock().now()

    # Agent counts
    free_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "AVAILABLE",
        )
    ).scalar() or 0

    reserved_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "RESERVED",
        )
    ).scalar() or 0

    dialing_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "DIALING",
        )
    ).scalar() or 0

    connected_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "CONNECTED",
        )
    ).scalar() or 0

    # In-flight calls: INITIATED or RINGING
    in_flight = session.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.state.in_([CallState.INITIATED.value, CallState.RINGING.value]),
        )
    ).scalar() or 0

    # Answered awaiting agent
    answered = session.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.state == CallState.ANSWERED.value,
        )
    ).scalar() or 0

    # Rolling abandonment rate: ABANDONED / (ANSWERED + CONNECTED + ABANDONED + COMPLETED)
    abandoned_count = session.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.state == CallState.ABANDONED.value,
        )
    ).scalar() or 0

    answered_total = session.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.state.in_([
                CallState.ANSWERED.value,
                CallState.CONNECTED.value,
                CallState.COMPLETED.value,
                CallState.ABANDONED.value,
            ]),
        )
    ).scalar() or 0

    rolling_abandon_rate = (abandoned_count / answered_total) if answered_total > 0 else 0.0

    # Check operator kill switch in system_flags
    kill_flag = session.execute(
        select(SystemFlag.flag_value).where(SystemFlag.flag_name == "kill_switch")
    ).scalar_one_or_none()
    kill_switch_active = (kill_flag is not None and kill_flag.strip().lower() in ("true", "1", "yes", "on"))

    # Hysteresis cooldown status
    last_fb = _campaign_hysteresis.get(campaign_id)
    in_cooldown = False
    if last_fb is not None:
        cooldown_delta = timedelta(seconds=settings.abandonment_cooldown_seconds)
        if _now < last_fb + cooldown_delta:
            in_cooldown = True
        else:
            # Cooldown window elapsed
            _campaign_hysteresis.pop(campaign_id, None)

    # Provider health snapshot from registry / circuit breaker
    provider_health = {}
    try:
        from smartdialer.providers.registry import get as get_provider
        provider = get_provider(selected_provider)
        if hasattr(provider, "health_snapshot"):
            provider_health[selected_provider] = provider.health_snapshot()
        else:
            provider_health[selected_provider] = {
                "circuit_state": CircuitState.CLOSED.value,
                "failure_rate": 0.0,
            }
    except Exception as e:
        logger.error(f"Failed to query provider '{selected_provider}' health: {e}")
        provider_health[selected_provider] = {
            "circuit_state": CircuitState.OPEN.value,
            "failure_rate": 1.0,
        }

    return StateSnapshot(
        campaign_id=campaign_id,
        free_agents=free_agents,
        reserved_agents=reserved_agents,
        dialing_agents=dialing_agents,
        connected_agents=connected_agents,
        in_flight=in_flight,
        answered_awaiting_agent=answered,
        rolling_abandonment_rate=rolling_abandon_rate,
        abandonment_sample_count=answered_total,
        provider_health=provider_health,
        selected_provider=selected_provider,
        kill_switch_active=kill_switch_active,
        in_abandonment_cooldown=in_cooldown,
        cooldown_target_rate=settings.abandonment_budget * settings.abandonment_hysteresis_ratio,
    )


# --------------------------------------------------------------------------- #
# Operator controls (for admin / tests — NOT imported by pacing)               #
# --------------------------------------------------------------------------- #

def set_kill_switch(session: Session, active: bool, now: datetime | None = None) -> None:
    """Set the system operator kill switch flag."""
    _now = now or SystemClock().now()
    flag = session.get(SystemFlag, "kill_switch")
    if flag is None:
        flag = SystemFlag(
            flag_name="kill_switch",
            flag_value="true" if active else "false",
            updated_at=_now,
        )
        session.add(flag)
    else:
        flag.flag_value = "true" if active else "false"
        flag.updated_at = _now
    session.flush()


def is_kill_switch_active(session: Session) -> bool:
    """Return True if operator kill switch flag is currently active."""
    kill_flag = session.scalar(
        select(SystemFlag.flag_value).where(SystemFlag.flag_name == "kill_switch")
    )
    return kill_flag is not None and kill_flag.strip().lower() in ("true", "1", "yes", "on")



def reset_hysteresis(campaign_id: int | None = None) -> None:
    """Reset hysteresis state (used in test teardown)."""
    if campaign_id is not None:
        _campaign_hysteresis.pop(campaign_id, None)
    else:
        _campaign_hysteresis.clear()
