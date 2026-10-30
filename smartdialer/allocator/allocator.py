"""
allocator/allocator.py — Progressive-dial call allocator.

Responsibility
--------------
Match available agents with eligible borrowers and create call rows in a
single atomic transaction.  The caller (orchestrator) owns the session and
transaction; this module never commits.

Why two workers cannot reserve the same agent
---------------------------------------------
1. SELECT ... FOR UPDATE SKIP LOCKED
   When worker A locks an agent row, Postgres marks that row as locked.
   Worker B's identical SELECT ... FOR UPDATE SKIP LOCKED will *skip* any
   row that is already locked by another transaction.  Worker B never even
   sees agent A is working on, so it cannot select it.

2. Partial unique index (uq_calls_active_agent in schema.sql)
   Even if the FOR UPDATE SKIP LOCKED were somehow bypassed (e.g. a bug
   causes two workers to see the same row in a brief window), the INSERT
   of a second active call row for the same agent_id would violate the
   partial unique index and raise IntegrityError.  This is the hard
   constraint backstop: correctness does not depend solely on the locking.

The two mechanisms are complementary:
  • FOR UPDATE SKIP LOCKED prevents the race at *read* time — high performance.
  • Partial unique index enforces the invariant at *write* time — correctness.

Contract
--------
• The caller provides an open Session (READ COMMITTED isolation).
• This function does NOT call session.commit() or session.rollback().
• If an exception is raised, the caller's transaction (and its context
  manager) will roll back all changes.
• Returns the list of Call UUIDs created in this tick.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK


def reserve_for_progressive(
    session: Session,
    campaign_id: int,
    max_n: int,
    now: datetime,
    provider: str = "mock",
) -> list[uuid.UUID]:
    """
    Allocate up to max_n calls for the given campaign in progressive mode.

    Progressive mode: one agent per one borrower.  The agent is reserved
    before the call is initiated so the borrower never hears hold music.

    Parameters
    ----------
    session     : Open SQLAlchemy session.  Caller owns commit/rollback.
    campaign_id : Campaign to dial for.
    max_n       : Upper bound on allocations this tick (from pacing engine).
    now         : Current timestamp (injected for testability).
    provider    : Provider tag written to the call row (default "mock").

    Returns
    -------
    List of Call UUIDs created.  May be shorter than max_n if fewer
    agents or borrowers are available.
    """
    # ------------------------------------------------------------------ #
    # Step 1: Lock AVAILABLE agents with SKIP LOCKED.                     #
    # ------------------------------------------------------------------ #
    # ORDER BY last_state_change_at ensures we pick the agent who has been
    # waiting longest (fairness); it also makes the order deterministic for
    # tests.
    agent_stmt = (
        select(Agent)
        .where(Agent.campaign_id == campaign_id, Agent.state == AgentState.AVAILABLE)
        .order_by(Agent.last_state_change_at)
        .limit(max_n)
        .with_for_update(skip_locked=True)
    )
    agents: Sequence[Agent] = session.scalars(agent_stmt).all()

    if not agents:
        return []

    # ------------------------------------------------------------------ #
    # Step 2: Lock ELIGIBLE borrowers with SKIP LOCKED.                   #
    # ------------------------------------------------------------------ #
    borrower_stmt = (
        select(Borrower)
        .where(
            Borrower.campaign_id == campaign_id,
            Borrower.state == "ELIGIBLE",
            # Only include borrowers whose cool-down period has elapsed.
            (Borrower.next_eligible_at == None) | (Borrower.next_eligible_at <= now),
        )
        .order_by(Borrower.priority.desc(), Borrower.next_eligible_at.asc())
        .limit(max_n)
        .with_for_update(skip_locked=True)
    )
    borrowers: Sequence[Borrower] = session.scalars(borrower_stmt).all()

    if not borrowers:
        return []

    # ------------------------------------------------------------------ #
    # Step 3: Pair agents and borrowers.                                  #
    # ------------------------------------------------------------------ #
    # k = min so we never have an unpaired agent or borrower becoming
    # RESERVED without a corresponding call row.
    k = min(len(agents), len(borrowers))
    agents_to_use = list(agents[:k])
    borrowers_to_use = list(borrowers[:k])

    # Rows locked beyond k are simply not touched — Postgres releases their
    # locks when this transaction commits (or rolls back).

    reservation_expires_at = now + timedelta(
        seconds=settings.agent_reservation_ttl_seconds
    )
    call_lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)

    created_call_ids: list[uuid.UUID] = []

    for agent, borrower in zip(agents_to_use, borrowers_to_use):
        call_id = uuid.uuid4()

        # Update agent: AVAILABLE -> RESERVED
        agent.state = AgentState.RESERVED
        agent.reserved_call_id = call_id
        agent.reservation_expires_at = reservation_expires_at
        agent.last_state_change_at = now
        agent.updated_at = now
        agent.version += 1

        # Update borrower: ELIGIBLE -> RESERVED
        borrower.state = "RESERVED"
        borrower.attempts += 1

        # Create call row in RESERVED state.
        call = Call(
            id=call_id,
            campaign_id=campaign_id,
            borrower_id=borrower.id,
            agent_id=agent.id,
            provider=provider,
            provider_call_id=None,
            state=CallState.RESERVED,
            state_rank=STATE_RANK[CallState.RESERVED],
            version=0,
            lease_expires_at=call_lease_expires_at,
            terminal_reason=None,
            created_at=now,
            updated_at=now,
        )
        session.add(call)
        created_call_ids.append(call_id)

    # session.flush() writes the INSERTs/UPDATEs to the DB within this
    # transaction so any constraint violations surface NOW (before commit)
    # rather than at commit time — giving cleaner error messages.
    session.flush()

    return created_call_ids


def release_reservation(
    session: Session,
    call_id: uuid.UUID,
    reason: str,
    now: datetime | None = None,
) -> None:
    """
    Roll back a reservation: return the agent to AVAILABLE and the borrower
    to ELIGIBLE, then move the call to CANCELLED.

    Called when:
    - An exception occurs after agents are reserved but before calls are sent.
    - The orchestrator decides to abort a tick.
    - The recovery sweeper reclaims an expired reservation.

    Parameters
    ----------
    session : Open session.  Caller owns commit/rollback.
    call_id : UUID of the call row to release.
    reason  : Human-readable reason stored in call.terminal_reason.
    now     : Timestamp to use; defaults to system clock.
    """
    if now is None:
        from smartdialer.clock import SystemClock
        now = SystemClock().now()

    call = session.get(Call, call_id, with_for_update=True)
    if call is None:
        return  # Already gone; nothing to do.

    # Only release non-terminal calls.
    if call.state in ("COMPLETED", "FAILED", "CANCELLED", "ABANDONED"):
        return

    # Release the agent.
    if call.agent_id is not None:
        agent = session.get(Agent, call.agent_id, with_for_update=True)
        if agent is not None and agent.state == AgentState.RESERVED:
            agent.state = AgentState.AVAILABLE
            agent.reserved_call_id = None
            agent.reservation_expires_at = None
            agent.last_state_change_at = now
            agent.updated_at = now
            agent.version += 1

    # Release the borrower.
    borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
    if borrower is not None and borrower.state == "RESERVED":
        borrower.state = "ELIGIBLE"

    # Cancel the call.
    call.state = CallState.CANCELLED
    call.state_rank = STATE_RANK[CallState.CANCELLED]
    call.terminal_reason = reason
    call.updated_at = now
    call.version += 1

    session.flush()


def reserve_with_safety_check(
    session: Session,
    request: "PacingRequest",
    advisory_decision: "SafetyDecision",
    now: datetime,
    provider: str = "mock_a",
) -> tuple[list[uuid.UUID], "SafetyDecision"]:
    """
    Two-phase check: allocator transaction calls safety.enforce against
    fresh in-transaction counts, then reserves.
    Enforce can only reduce or reject, never increase.
    """
    from smartdialer.safety.controller import enforce

    enforce_decision = enforce(
        session,
        request,
        selected_provider=provider,
        now=now,
        advisory_limit=advisory_decision.approved_calls,
    )
    if enforce_decision.approved_calls <= 0:
        return [], enforce_decision

    call_ids = []
    # If mode is PREDICTIVE and safety did NOT force fallback to PROGRESSIVE:
    from smartdialer.safety.controller import PacingMode, SafetyAction

    if (
        request.mode == PacingMode.PREDICTIVE
        and enforce_decision.action != SafetyAction.FALLBACK_PROGRESSIVE
    ):
        call_ids = reserve_for_predictive(
            session,
            campaign_id=request.campaign_id,
            max_n=enforce_decision.approved_calls,
            now=now,
            provider=provider,
        )
    else:
        call_ids = reserve_for_progressive(
            session,
            campaign_id=request.campaign_id,
            max_n=enforce_decision.approved_calls,
            now=now,
            provider=provider,
        )
    return call_ids, enforce_decision


def reserve_for_predictive(
    session: Session,
    campaign_id: int,
    max_n: int,
    now: datetime,
    provider: str = "mock_a",
) -> list[uuid.UUID]:
    """
    Allocate up to max_n calls for predictive dialing.
    Predictive calls do NOT reserve an agent up front (agent_id = None).
    An agent is claimed dynamically at answer-time.
    """
    if max_n <= 0:
        return []

    borrower_stmt = (
        select(Borrower)
        .where(
            Borrower.campaign_id == campaign_id,
            Borrower.state == "ELIGIBLE",
            (Borrower.next_eligible_at == None) | (Borrower.next_eligible_at <= now),
        )
        .order_by(Borrower.priority.desc(), Borrower.next_eligible_at.asc())
        .limit(max_n)
        .with_for_update(skip_locked=True)
    )
    borrowers: Sequence[Borrower] = session.scalars(borrower_stmt).all()
    if not borrowers:
        return []

    call_lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)
    created_call_ids: list[uuid.UUID] = []

    for borrower in borrowers:
        call_id = uuid.uuid4()
        borrower.state = "RESERVED"
        borrower.attempts += 1

        call = Call(
            id=call_id,
            campaign_id=campaign_id,
            borrower_id=borrower.id,
            agent_id=None,  # No agent reserved up front
            provider=provider,
            provider_call_id=None,
            state=CallState.RESERVED.value,
            state_rank=STATE_RANK[CallState.RESERVED],
            version=0,
            lease_expires_at=call_lease_expires_at,
            created_at=now,
            updated_at=now,
        )
        session.add(call)
        created_call_ids.append(call_id)

    session.flush()
    return created_call_ids

