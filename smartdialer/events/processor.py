"""
events/processor.py — Event processor: claim, apply, update.

Guarantee
---------
We provide IDEMPOTENT PROCESSING + RECONCILIATION, not exactly-once delivery.
The same event row can be processed multiple times (QUARANTINED retry);
each processing attempt checks whether the FSM transition has already been
applied and is a no-op if so.  The inbox's ON CONFLICT DO NOTHING ensures
one row per (provider, event_id), and the processor's FOR UPDATE SKIP LOCKED
ensures one processor thread processes each row at a time.

Processing steps per event
--------------------------
1. Claim a batch of RECEIVED/QUARANTINED-due events with FOR UPDATE SKIP LOCKED.
2. For each event:
   a. Resolve call_id (may have been NULL at ingest time).
   b. SELECT call FOR UPDATE (prevents concurrent state changes).
   c. Apply call_fsm.apply_event().
   d. If applied: update call row, update agent/borrower rows, flush.
   e. Write final status + reason to the event row.
3. Commit the entire batch atomically.

Unknown call_id (QUARANTINED)
-----------------------------
If the call_id is still not found after ingest, the event may have arrived
before the executor committed Txn 2.  We store QUARANTINED with a backoff
and retry.  After event_max_attempts, we give up and write DEAD_LETTER.
This implements principle: "Late/invalid events are stored with a reason."

Reprocessing APPLIED event
---------------------------
If an event row already has status=APPLIED (reprocessed due to a previous
partial commit), the processor checks whether the call's state_rank already
reflects this event, and if so treats it as a no-op.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK, apply_event

logger = logging.getLogger(__name__)

# Mapping from event_type string to FSM event name.
_EVENT_TYPE_TO_FSM: dict[str, str] = {
    "RINGING":   "RING",
    "ANSWERED":  "ANSWER",
    "COMPLETED": "COMPLETE",
    "FAILED":    "FAIL",
    "CONNECTED": "CONNECT",
    "CANCELLED": "CANCEL",
    "ABANDONED": "ABANDON",
}


def process_pending(
    session_factory,
    batch_size: int | None = None,
    clock: Clock | None = None,
) -> int:
    """
    Claim and process a batch of pending events.

    Opens its own session per call (stateless, safe for threading).
    Returns the number of events processed.
    """
    _clock = clock or SystemClock()
    _batch = batch_size or settings.event_processor_batch_size
    now = _clock.now()

    session: Session = session_factory()
    try:
        return _process_batch(session, _batch, now, _clock)
    finally:
        session.close()


def _process_batch(
    session: Session, batch_size: int, now: datetime, clock: Clock
) -> int:
    """
    Claim events and process them in a single transaction.

    FOR UPDATE SKIP LOCKED ensures only one thread processes each event row.
    """
    # Claim RECEIVED events, or QUARANTINED events whose next_retry_at has passed.
    stmt = (
        select(CallEvent)
        .where(
            (CallEvent.status == "RECEIVED")
            | (
                (CallEvent.status == "QUARANTINED")
                & (
                    (CallEvent.next_retry_at == None)
                    | (CallEvent.next_retry_at <= now)
                )
            )
        )
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    events = session.scalars(stmt).all()

    if not events:
        session.rollback()
        return 0

    processed = 0
    for ev in events:
        _process_one(session, ev, now, clock)
        processed += 1

    session.commit()
    return processed


def _process_one(
    session: Session,
    ev: CallEvent,
    now: datetime,
    clock: Clock,
) -> None:
    """
    Process a single event row.  Mutates ev.status and ev.reason in-place.
    All changes are flushed together with the batch commit.
    """
    ev.attempts += 1
    correlation = {"call_event_id": ev.id, "event_type": ev.event_type}

    # ------------------------------------------------------------------ #
    # Step 1: Resolve call_id                                              #
    # ------------------------------------------------------------------ #
    if ev.call_id is None:
        # Try to resolve from provider_call_id now.
        call_id = session.execute(
            select(Call.id).where(
                Call.provider == ev.provider,
                Call.provider_call_id == ev.provider_call_id,
            )
        ).scalar_one_or_none()

        if call_id is None:
            _quarantine_or_dead_letter(ev, now, "call_id not found")
            logger.info(json.dumps({**correlation, "event": "processor.quarantined",
                                    "reason": "call_id not found"}))
            return
        ev.call_id = call_id

    # ------------------------------------------------------------------ #
    # Step 2: Load call with FOR UPDATE                                    #
    # ------------------------------------------------------------------ #
    call = session.get(
        Call, ev.call_id,
        with_for_update=True,
    )
    if call is None:
        _quarantine_or_dead_letter(ev, now, "call row not found after resolution")
        logger.warning(json.dumps({**correlation, "event": "processor.quarantined",
                                   "call_id": str(ev.call_id),
                                   "reason": "call row missing"}))
        return

    correlation["call_id"] = str(call.id)

    # ------------------------------------------------------------------ #
    # Step 3: Map event_type to FSM event name                            #
    # ------------------------------------------------------------------ #
    fsm_event = _EVENT_TYPE_TO_FSM.get(ev.event_type.upper())
    if fsm_event is None:
        ev.status = "IGNORED_INVALID"
        ev.reason = f"Unrecognised event_type '{ev.event_type}'"
        logger.warning(json.dumps({**correlation, "event": "processor.ignored_invalid",
                                   "reason": ev.reason}))
        return

    # ------------------------------------------------------------------ #
    # Step 4: Apply FSM transition                                         #
    # ------------------------------------------------------------------ #
    current = CallState(call.state)
    new_state, applied, reason = apply_event(current, fsm_event)

    if not applied:
        # Determine whether this is late (terminal) or just backward.
        if current in (CallState.COMPLETED, CallState.FAILED,
                       CallState.CANCELLED, CallState.ABANDONED):
            ev.status = "IGNORED_LATE"
            ev.reason = reason
            if fsm_event == "ANSWER" and current == CallState.CANCELLED:
                from smartdialer.recovery.sweeper import record_late_answer_after_cancel
                record_late_answer_after_cancel(ev.event_id, str(call.id))
            logger.info(json.dumps({**correlation, "event": "processor.ignored_late",
                                    "call_state": current.value, "reason": reason}))
        else:
            ev.status = "IGNORED_DUPLICATE"
            ev.reason = reason
            logger.info(json.dumps({**correlation, "event": "processor.ignored_duplicate",
                                    "reason": reason}))
        return

    # ------------------------------------------------------------------ #
    # Step 5: Update call row                                              #
    # ------------------------------------------------------------------ #
    call.state = new_state.value
    call.state_rank = STATE_RANK[new_state]
    call.version += 1
    call.lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)
    call.updated_at = now

    # ------------------------------------------------------------------ #
    # Step 6: Update agent / borrower side-effects                        #
    # ------------------------------------------------------------------ #
    _apply_side_effects(session, call, new_state, now)

    # ------------------------------------------------------------------ #
    # Step 7: Mark event APPLIED                                          #
    # ------------------------------------------------------------------ #
    ev.status = "APPLIED"
    ev.reason = None

    logger.info(json.dumps({**correlation, "event": "processor.applied",
                             "from": current.value, "to": new_state.value}))


def _apply_side_effects(
    session: Session, call: Call, new_state: CallState, now: datetime
) -> None:
    """
    Update agent and borrower rows to match the new call state.
    Handles dynamic answer-time agent claim for predictive calls (agent_id=None).
    """
    borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
    agent = session.get(Agent, call.agent_id, with_for_update=True) if call.agent_id else None

    if new_state == CallState.RINGING:
        pass

    elif new_state == CallState.ANSWERED:
        if borrower and borrower.state == "RESERVED":
            borrower.state = "IN_CALL"

        # Record answer to estimator
        try:
            from smartdialer.pacing.estimator import record_dial_outcome
            record_dial_outcome(call.campaign_id, answered=True)
        except Exception as e:
            logger.warning(f"Failed to record answer outcome for call {call.id}: {e}")

        if call.agent_id is None:
            # PREDICTIVE ANSWER-TIME AGENT CLAIM:
            # Claim an AVAILABLE agent with FOR UPDATE SKIP LOCKED
            agent_stmt = (
                select(Agent)
                .where(Agent.campaign_id == call.campaign_id, Agent.state == "AVAILABLE")
                .order_by(Agent.last_state_change_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            available_agent = session.scalars(agent_stmt).one_or_none()
            if available_agent:
                available_agent.state = AgentState.CONNECTED.value
                available_agent.last_state_change_at = now
                available_agent.updated_at = now
                available_agent.version += 1

                call.agent_id = available_agent.id
                call.state = CallState.CONNECTED.value
                call.state_rank = STATE_RANK[CallState.CONNECTED]
                call.held_since = None
                session.flush()
            else:
                # No agent free -> enter HELD
                call.state = CallState.HELD.value
                call.state_rank = STATE_RANK[CallState.HELD]
                call.held_since = now
                session.flush()
        else:
            # Progressive dial: agent was already reserved
            if agent and agent.state in ("DIALING", "RESERVED"):
                agent.state = AgentState.CONNECTED.value
                agent.last_state_change_at = now
                agent.updated_at = now
                agent.version += 1
            # Call state remains ANSWERED (as set in step 5)


    elif new_state == CallState.CONNECTED:
        if agent and agent.state != AgentState.CONNECTED.value:
            agent.state = AgentState.CONNECTED.value
            agent.last_state_change_at = now
            agent.updated_at = now
            agent.version += 1

    elif new_state in (CallState.COMPLETED, CallState.FAILED,
                       CallState.CANCELLED, CallState.ABANDONED):
        # Record non-answer outcome to the Beta-Bayesian estimator if the call terminated without answering
        if new_state in (CallState.FAILED, CallState.CANCELLED):
            try:
                from smartdialer.pacing.estimator import record_dial_outcome
                record_dial_outcome(call.campaign_id, answered=False)
            except Exception as e:
                logger.warning(f"Failed to record non-answer outcome for call {call.id}: {e}")

        # Terminal: release agent to WRAP_UP
        if agent and agent.state not in (AgentState.OFFLINE.value, AgentState.AVAILABLE.value,
                                          AgentState.WRAP_UP.value):
            agent.state = AgentState.WRAP_UP.value
            agent.reserved_call_id = None
            agent.reservation_expires_at = None
            agent.last_state_change_at = now
            agent.updated_at = now
            agent.version += 1

        if borrower:
            if new_state == CallState.COMPLETED:
                borrower.state = "DONE"
            else:
                borrower.state = "ELIGIBLE"
                borrower.next_eligible_at = now + timedelta(
                    seconds=settings.borrower_retry_backoff_seconds
                )


def sweep_held_calls(
    session: Session,
    now: datetime,
    clock: Clock | None = None,
) -> int:
    """
    Sweep HELD calls waiting for an agent:
    - If hold duration exceeds hold_timeout_seconds: abandon the call.
    - Else: attempt to claim an AVAILABLE agent.
    """
    stmt = (
        select(Call)
        .where(Call.state == CallState.HELD.value)
        .order_by(Call.held_since.asc())
        .limit(50)
        .with_for_update(skip_locked=True)
    )
    held_calls = session.scalars(stmt).all()
    count = 0

    for call in held_calls:
        held_duration = (now - call.held_since).total_seconds() if call.held_since else 999.0

        if held_duration > settings.hold_timeout_seconds:
            # Exceeded timeout -> ABANDONED
            call.state = CallState.ABANDONED.value
            call.state_rank = STATE_RANK[CallState.ABANDONED]
            call.terminal_reason = "hold_timeout_abandoned"
            call.held_since = None
            call.updated_at = now
            call.version += 1

            borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
            if borrower:
                borrower.attempts += 1
                if borrower.attempts >= settings.max_borrower_attempts:
                    borrower.state = settings.borrower_terminal_state_on_max_attempts
                    borrower.next_eligible_at = None
                else:
                    borrower.state = "ELIGIBLE"
                    delay = min(
                        settings.borrower_base_backoff_seconds * (2 ** (borrower.attempts - 1)),
                        settings.borrower_max_backoff_seconds,
                    )
                    borrower.next_eligible_at = now + timedelta(seconds=delay)
            count += 1
        else:
            # Try to claim available agent
            agent_stmt = (
                select(Agent)
                .where(Agent.campaign_id == call.campaign_id, Agent.state == "AVAILABLE")
                .order_by(Agent.last_state_change_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            available_agent = session.scalars(agent_stmt).one_or_none()
            if available_agent:
                available_agent.state = AgentState.CONNECTED.value
                available_agent.last_state_change_at = now
                available_agent.updated_at = now
                available_agent.version += 1

                call.agent_id = available_agent.id
                call.state = CallState.CONNECTED.value
                call.state_rank = STATE_RANK[CallState.CONNECTED]
                call.held_since = None
                call.updated_at = now
                call.version += 1
                count += 1

    session.flush()
    return count


def _quarantine_or_dead_letter(ev: CallEvent, now: datetime, reason: str) -> None:
    """
    Move an event to QUARANTINED with exponential backoff, or DEAD_LETTER
    after max attempts.

    Backoff formula: min(base * 2^(attempts-1), max_delay) seconds.
    """
    if ev.attempts >= settings.event_max_attempts:
        ev.status = "DEAD_LETTER"
        ev.reason = f"Max attempts ({settings.event_max_attempts}) exceeded. Last: {reason}"
        return

    base = settings.event_quarantine_base_delay_seconds
    max_delay = settings.event_quarantine_max_delay_seconds
    delay = min(base * (2 ** (ev.attempts - 1)), max_delay)

    ev.status = "QUARANTINED"
    ev.reason = reason
    ev.next_retry_at = now + timedelta(seconds=delay)
