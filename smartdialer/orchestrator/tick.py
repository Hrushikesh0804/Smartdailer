"""
smartdialer.orchestrator.tick — Single tick execution pipeline.

Pipeline Order
--------------
1. capacity_guard: check if capacity < in_flight; cancel excess RINGING calls newest-first.
2. hold_sweep: bridge newly available agents to HELD calls or abandon expired ones.
3. snapshot: build StateSnapshot from live DB counts.
4. pacing: compute proposal (progressive or predictive).
5. safety.advise: advisory safety check (pure function).
6. allocator (transaction): safety.enforce + reserve.
7. dial_executor: initiate calls outside transactions.
8. process_pending: ingest/process pending telecom events.

Stateless between ticks. All state is maintained in PostgreSQL.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from smartdialer.allocator.allocator import reserve_with_safety_check
from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import MetricsSnapshot
from smartdialer.db.session import transaction
from smartdialer.events.processor import process_pending, sweep_held_calls
from smartdialer.orchestrator.capacity_guard import check_and_guard_capacity
from smartdialer.orchestrator.dial_executor import DialExecutor
from smartdialer.pacing.predictive import compute_predictive_proposal
from smartdialer.pacing.progressive import calculate_request
from smartdialer.providers.registry import get as get_provider
from smartdialer.safety.controller import PacingMode, PacingRequest, advise, build_snapshot

logger = logging.getLogger(__name__)


def run_tick(
    session_factory,
    campaign_id: int,
    provider_name: str = "mock_a",
    mode: str = "PROGRESSIVE",
    clock: Clock | None = None,
    fixed_rate: float | None = None,
) -> dict[str, Any]:
    """
    Execute one complete tick for a given campaign.
    """
    _clock = clock or SystemClock()
    now = _clock.now()

    # ------------------------------------------------------------------ #
    # Step 1: Capacity Guard & Hold Sweep                                #
    # ------------------------------------------------------------------ #
    guard_result = check_and_guard_capacity(
        session_factory=session_factory,
        campaign_id=campaign_id,
        now=now,
        provider_name=provider_name,
    )

    held_swept = 0
    sweep_session = session_factory()
    try:
        with transaction(sweep_session):
            held_swept = sweep_held_calls(sweep_session, now=now, clock=_clock)
    finally:
        sweep_session.close()

    # ------------------------------------------------------------------ #
    # Step 2: Snapshot                                                   #
    # ------------------------------------------------------------------ #
    read_session = session_factory()
    try:
        snapshot = build_snapshot(
            read_session,
            campaign_id=campaign_id,
            selected_provider=provider_name,
            now=now,
        )
    finally:
        read_session.close()

    # ------------------------------------------------------------------ #
    # Step 3: Pacing Proposal                                            #
    # ------------------------------------------------------------------ #
    mode_upper = mode.upper()
    is_predictive = mode_upper in (PacingMode.PREDICTIVE.value, "PREDICTIVE_ADAPTIVE", "PREDICTIVE_FIXED")
    pacing_why = ""
    if is_predictive:
        pacing_session = session_factory()
        try:
            with transaction(pacing_session):
                pacing_dec = compute_predictive_proposal(
                    pacing_session,
                    campaign_id,
                    clock=_clock,
                    fixed_rate=fixed_rate,
                )
            pacing_why = pacing_dec.why
            pacing_request = PacingRequest(
                campaign_id=campaign_id,
                mode=PacingMode.PREDICTIVE,
                requested_calls=pacing_dec.requested_calls,
                estimate_meta=pacing_dec.inputs,
            )
        finally:
            pacing_session.close()
    else:
        pacing_request = calculate_request(snapshot)
        pacing_why = f"progressive: requested {pacing_request.requested_calls} to match free agents"


    # ------------------------------------------------------------------ #
    # Step 4: Safety Advisory                                            #
    # ------------------------------------------------------------------ #
    advisory_decision = advise(snapshot, pacing_request)

    # ------------------------------------------------------------------ #
    # Step 5: Allocator Transaction (Enforce + Reserve)                  #
    # ------------------------------------------------------------------ #
    allocated_call_ids: list[uuid.UUID] = []
    enforced_action = advisory_decision.action.value
    approved_count = 0

    alloc_session = session_factory()
    try:
        with transaction(alloc_session):
            allocated_call_ids, enforce_decision = reserve_with_safety_check(
                session=alloc_session,
                request=pacing_request,
                advisory_decision=advisory_decision,
                now=now,
                provider=provider_name,
            )
            enforced_action = enforce_decision.action.value
            approved_count = enforce_decision.approved_calls
    finally:
        alloc_session.close()

    # ------------------------------------------------------------------ #
    # Step 6: Dial Executor (Outside DB transactions)                    #
    # ------------------------------------------------------------------ #
    dialed_count = 0
    if allocated_call_ids:
        provider = get_provider(provider_name)
        executor = DialExecutor(
            provider=provider,
            session_factory=session_factory,
            clock=_clock,
        )
        for cid in allocated_call_ids:
            executor.execute(cid)
            dialed_count += 1

    # ------------------------------------------------------------------ #
    # Step 7: Process Pending Events & Hold Sweep                        #
    # ------------------------------------------------------------------ #
    events_processed = process_pending(
        session_factory=session_factory,
        batch_size=settings.event_processor_batch_size,
        clock=_clock,
    )

    # ------------------------------------------------------------------ #
    # Step 8: Record Metrics Snapshot for Dashboard                       #
    # ------------------------------------------------------------------ #
    try:
        snap_session = session_factory()
        try:
            with transaction(snap_session):
                total_online = snapshot.free_agents + snapshot.reserved_agents + snapshot.dialing_agents + snapshot.connected_agents
                busy_agents = snapshot.reserved_agents + snapshot.dialing_agents + snapshot.connected_agents
                util_pct = (busy_agents / max(1, total_online)) * 100.0

                meta = pacing_request.estimate_meta or {}
                p_m = float(meta.get("p_mean", 0.0))
                p_h = float(meta.get("p_hi", 0.0))

                snap = MetricsSnapshot(
                    campaign_id=campaign_id,
                    in_flight=guard_result["in_flight"],
                    capacity=guard_result["capacity"],
                    connected=snapshot.connected_agents,
                    abandoned=snapshot.abandonment_sample_count,
                    utilization_pct=round(util_pct, 2),
                    p_mean=round(p_m, 4),
                    p_hi=round(p_h, 4),
                    created_at=now,
                )
                snap_session.add(snap)
        finally:
            snap_session.close()
    except Exception as e:
        logger.debug(f"Metrics snapshot recording failed: {e}")

    summary = {
        "campaign_id": campaign_id,
        "mode": mode,
        "free_agents": snapshot.free_agents,
        "capacity": guard_result["capacity"],
        "in_flight": guard_result["in_flight"],
        "excess_cancelled": guard_result["cancelled"],
        "held_swept": held_swept,
        "requested_calls": pacing_request.requested_calls,
        "pacing_why": pacing_why,
        "advisory_action": advisory_decision.action.value,
        "advisory_rule_ids": advisory_decision.rule_ids,
        "enforced_action": enforced_action,
        "approved_calls": approved_count,
        "allocated_calls": len(allocated_call_ids),
        "dialed_calls": dialed_count,
        "events_processed": events_processed,
    }
    logger.debug(json.dumps({"event": "tick.completed", **summary}))
    return summary

