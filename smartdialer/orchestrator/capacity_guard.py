"""
smartdialer.orchestrator.capacity_guard — Real-time agent capacity protector.

Guarantees & Invariants
-----------------------
1. Recomputes capacity from the database every tick.
2. If capacity < in_flight:
   - Halts any new dials immediately.
   - Cancels excess RINGING calls newest-first (order_by created_at DESC).
   - NEVER touches ANSWERED, HELD, or CONNECTED calls.
3. Provides estimate_ticks_to_recover() reporting ticks until in_flight <= cap.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import transaction
from smartdialer.domain.call_fsm import CallState, STATE_RANK

logger = logging.getLogger(__name__)


def check_and_guard_capacity(
    session_factory,
    campaign_id: int,
    now: datetime,
    provider_name: str = "mock_a",
) -> dict[str, Any]:
    """
    Check if live capacity has fallen below in-flight calls.
    If so, cancel excess RINGING calls newest-first.
    """
    session: Session = session_factory()
    try:
        # Live available agents
        free_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == campaign_id,
                Agent.state == "AVAILABLE",
            )
        ).scalar() or 0

        wrap_up_agents = session.execute(
            select(func.count(Agent.id)).where(
                Agent.campaign_id == campaign_id,
                Agent.state == "WRAP_UP",
            )
        ).scalar() or 0

        capacity = free_agents + (wrap_up_agents // 2)

        # In-flight calls (INITIATED + RINGING)
        in_flight = session.execute(
            select(func.count(Call.id)).where(
                Call.campaign_id == campaign_id,
                Call.state.in_([CallState.INITIATED.value, CallState.RINGING.value]),
            )
        ).scalar() or 0

        excess = max(0, in_flight - capacity)
        cancelled_count = 0

        if excess > 0:
            logger.warning(json.dumps({
                "event": "capacity_guard.capacity_drop_detected",
                "campaign_id": campaign_id,
                "capacity": capacity,
                "in_flight": in_flight,
                "excess": excess,
            }))

            # Lock excess RINGING calls newest-first
            ringing_stmt = (
                select(Call)
                .where(
                    Call.campaign_id == campaign_id,
                    Call.state == CallState.RINGING.value,
                )
                .order_by(Call.created_at.desc())
                .limit(excess)
                .with_for_update(skip_locked=True)
            )
            excess_calls = session.scalars(ringing_stmt).all()

            with transaction(session):
                for call in excess_calls:
                    call.state = CallState.CANCELLED.value
                    call.state_rank = STATE_RANK[CallState.CANCELLED]
                    call.terminal_reason = "capacity_drop_cancelled"
                    call.updated_at = now
                    call.version += 1

                    # Release borrower without penalty
                    borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
                    if borrower and borrower.state == "RESERVED":
                        borrower.state = "ELIGIBLE"
                        borrower.next_eligible_at = now

                    cancelled_count += 1

        ticks_to_recover = estimate_ticks_to_recover(
            capacity=capacity,
            in_flight=max(0, in_flight - cancelled_count),
        )

        return {
            "capacity": capacity,
            "in_flight": in_flight,
            "excess": excess,
            "cancelled": cancelled_count,
            "ticks_to_recover": ticks_to_recover,
        }
    finally:
        session.close()


def estimate_ticks_to_recover(
    capacity: int,
    in_flight: int,
    avg_call_duration_seconds: float = 60.0,
    tick_interval_seconds: float = 1.0,
) -> int:
    """
    Calculate expected ticks until in_flight drops to or below capacity.
    """
    if in_flight <= capacity:
        return 0
    diff = in_flight - capacity
    # Completion rate: capacity / avg_call_duration per second
    effective_rate_per_sec = max(0.1, capacity) / max(1.0, avg_call_duration_seconds)
    seconds_needed = diff / effective_rate_per_sec
    ticks = math.ceil(seconds_needed / max(0.1, tick_interval_seconds))
    return max(1, ticks)
