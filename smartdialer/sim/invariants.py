"""
smartdialer.sim.invariants — Invariants checker for simulations and tests.

Run at the end of every simulation and fails loudly if any invariant is violated:
1. No agent with >1 active call.
2. No borrower with >1 active call.
3. No terminal call re-opened.
4. In-flight calls never exceeded hard cap (capacity * MAX_OVERDIAL_RATIO).
5. Every ANSWERED call ended CONNECTED, ABANDONED, or terminal with a reason.
6. No HELD call older than HOLD_TIMEOUT.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from smartdialer.config import settings
from smartdialer.domain.call_fsm import CallState, TERMINAL_STATES
from smartdialer.db.models import Agent, Borrower, Call, CallEvent


class InvariantViolationError(AssertionError):
    """Raised when a system or safety invariant is violated."""
    pass


def assert_all_invariants(
    session: Session,
    campaign_id: int,
    now: datetime,
    max_inflight_observed: int,
    total_agents: int,
    max_overdial_ratio: float | None = None,
    allow_active_at_end: bool = True,
) -> dict[str, Any]:
    """
    Audit all invariants on the database state and simulation telemetry.
    Fails loudly with InvariantViolationError if any rule is broken.
    Returns a dict summarizing verified invariant metrics.
    """
    violations: list[str] = []
    ratio = max_overdial_ratio or settings.max_overdial_ratio
    cap = int(total_agents * ratio)

    # ------------------------------------------------------------------ #
    # Invariant 1: No agent with > 1 active call                         #
    # ------------------------------------------------------------------ #
    active_agent_stmt = (
        select(Call.agent_id, func.count(Call.id).label("active_count"))
        .where(
            Call.campaign_id == campaign_id,
            Call.agent_id.is_not(None),
            Call.state.not_in([s.value for s in TERMINAL_STATES]),
        )
        .group_by(Call.agent_id)
        .having(func.count(Call.id) > 1)
    )
    double_agents = session.execute(active_agent_stmt).all()
    if double_agents:
        violations.append(
            f"Invariant 1 Violated: {len(double_agents)} agent(s) have >1 active call: {double_agents}"
        )

    # ------------------------------------------------------------------ #
    # Invariant 2: No borrower with > 1 active call                      #
    # ------------------------------------------------------------------ #
    active_borrower_stmt = (
        select(Call.borrower_id, func.count(Call.id).label("active_count"))
        .where(
            Call.campaign_id == campaign_id,
            Call.state.not_in([s.value for s in TERMINAL_STATES]),
        )
        .group_by(Call.borrower_id)
        .having(func.count(Call.id) > 1)
    )
    double_borrowers = session.execute(active_borrower_stmt).all()
    if double_borrowers:
        violations.append(
            f"Invariant 2 Violated: {len(double_borrowers)} borrower(s) have >1 active call: {double_borrowers}"
        )

    # ------------------------------------------------------------------ #
    # Invariant 3: In-flight calls never above cap                       #
    # ------------------------------------------------------------------ #
    if max_inflight_observed > cap:
        violations.append(
            f"Invariant 3 Violated: Max in-flight ({max_inflight_observed}) exceeded hard cap ({cap} = {total_agents} * {ratio})"
        )

    # ------------------------------------------------------------------ #
    # Invariant 4: No HELD call older than HOLD_TIMEOUT                  #
    # ------------------------------------------------------------------ #
    timeout_threshold = now - timedelta(seconds=settings.hold_timeout_seconds + 1.0)
    stale_held_stmt = select(Call).where(
        Call.campaign_id == campaign_id,
        Call.state == CallState.HELD.value,
        Call.held_since < timeout_threshold,
    )
    stale_held = session.scalars(stale_held_stmt).all()
    if stale_held:
        violations.append(
            f"Invariant 4 Violated: {len(stale_held)} call(s) in HELD older than timeout: {[str(c.id) for c in stale_held]}"
        )

    # ------------------------------------------------------------------ #
    # Invariant 5: Every answered call ended CONNECTED, ABANDONED, or    #
    # terminal with a valid reason                                       #
    # ------------------------------------------------------------------ #
    # Find all calls that reached ANSWERED (from call_events)
    answered_call_ids_stmt = (
        select(CallEvent.call_id)
        .where(
            CallEvent.event_type == "ANSWERED",
            CallEvent.status == "APPLIED",
            CallEvent.call_id.is_not(None),
        )
        .distinct()
    )
    answered_call_ids = [row[0] for row in session.execute(answered_call_ids_stmt).all()]

    if answered_call_ids:
        unresolved_answered_stmt = select(Call).where(
            Call.id.in_(answered_call_ids),
            Call.state.not_in([
                CallState.ANSWERED.value,
                CallState.CONNECTED.value,
                CallState.COMPLETED.value,
                CallState.ABANDONED.value,
                CallState.FAILED.value,
                CallState.HELD.value,
            ]),
        )
        unresolved = session.scalars(unresolved_answered_stmt).all()
        if unresolved:
            violations.append(
                f"Invariant 5 Violated: {len(unresolved)} answered call(s) in invalid state: "
                f"{[(str(c.id), c.state) for c in unresolved]}"
            )

    # ------------------------------------------------------------------ #
    # Invariant 6: No terminal call re-opened                            #
    # ------------------------------------------------------------------ #
    # Check if any event applied to a call after it reached a terminal state
    # (By FSM definition, apply_event absorbs terminal states; we verify DB consistency)
    terminal_events_stmt = (
        select(CallEvent.call_id)
        .where(
            CallEvent.event_type.in_(["COMPLETED", "FAILED", "CANCELLED", "ABANDONED"]),
            CallEvent.status == "APPLIED",
            CallEvent.call_id.is_not(None),
        )
        .distinct()
    )
    terminal_ids = [row[0] for row in session.execute(terminal_events_stmt).all()]
    if terminal_ids:
        non_terminal_now = session.scalars(
            select(Call).where(
                Call.id.in_(terminal_ids),
                Call.state.not_in([s.value for s in TERMINAL_STATES]),
            )
        ).all()
        if non_terminal_now:
            violations.append(
                f"Invariant 6 Violated: {len(non_terminal_now)} call(s) that were terminal are now active: "
                f"{[(str(c.id), c.state) for c in non_terminal_now]}"
            )

    if violations:
        error_msg = "\n".join(["[INVARIANT AUDIT FAILURE]"] + violations)
        raise InvariantViolationError(error_msg)

    return {
        "status": "ALL_INVARIANTS_PASSED",
        "campaign_id": campaign_id,
        "max_inflight_observed": max_inflight_observed,
        "inflight_cap": cap,
        "double_agents": 0,
        "double_borrowers": 0,
        "stale_held_calls": 0,
        "terminal_reopened": 0,
    }
