"""
SQLAlchemy ORM models (SQLAlchemy 2.x Mapped[] style).

Design notes
------------
* All tables use BigInteger PKs except calls, which uses UUID to make
  idempotency keys globally unique without coordination.
* `version` columns enable optimistic locking: callers read the version,
  do business logic, then UPDATE ... WHERE version = <old>; if rowcount==0
  a concurrent writer won the race.
* `state_rank` on Call mirrors the integer rank of the state enum and allows
  a cheap "did this event move us forward?" check without string comparisons.
* `phone_token` on Borrower is an opaque token — never a real phone number —
  so the model layer never handles PII directly.
* `reconcile_deadline` on Call: when a provider initiation times out (UNKNOWN
  state), the sweeper must act by this deadline.  After the deadline, if still
  UNKNOWN, the sweeper cancels or forces FAILED with a terminal_reason.
* CallEvent is the events inbox: each row is one raw provider event.  It has
  a UNIQUE(provider, event_id) so the INSERT ... ON CONFLICT DO NOTHING dedup
  works at the DB level even under concurrent ingestion.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# --------------------------------------------------------------------------- #
# Agent                                                                        #
# --------------------------------------------------------------------------- #

class Agent(Base):
    """
    Represents a human call-centre agent.

    State machine: OFFLINE -> AVAILABLE -> RESERVED -> DIALING -> CONNECTED
                   -> WRAP_UP -> AVAILABLE (cycle)
                   Any state -> PAUSED -> (resume) -> previous active state
                   Any state -> OFFLINE

    `reserved_call_id` and `reservation_expires_at` are set when state=RESERVED
    so the recovery sweeper can detect and release stale reservations without
    joining the calls table.
    """

    __tablename__ = "agents"
    __table_args__ = (
        CheckConstraint(
            "state IN ('OFFLINE','AVAILABLE','RESERVED','DIALING',"
            "'CONNECTED','WRAP_UP','PAUSED')",
            name="ck_agent_state",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    team_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    state: Mapped[str] = mapped_column(String(20), nullable=False, default="OFFLINE")

    # Optimistic-lock counter.  Increment on every state change.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Populated when state=RESERVED so the sweeper can reclaim without a join.
    reserved_call_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    reservation_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    last_state_change_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    # Relationship (back-populated from Call)
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="agent")


# --------------------------------------------------------------------------- #
# Borrower                                                                     #
# --------------------------------------------------------------------------- #

class Borrower(Base):
    """
    A person to be contacted.

    `phone_token` is an opaque reference resolved only at dial-time by the
    provider adapter, keeping PII out of this layer.

    State:
      ELIGIBLE  -> can be selected by the allocator
      RESERVED  -> an active call row exists; do not double-dial
      IN_CALL   -> call is ANSWERED or CONNECTED
      DONE      -> successfully contacted; no further attempts
      DO_NOT_CALL -> removed from all campaigns
    """

    __tablename__ = "borrowers"
    __table_args__ = (
        CheckConstraint(
            "state IN ('ELIGIBLE','RESERVED','IN_CALL','DONE','DO_NOT_CALL')",
            name="ck_borrower_state",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    # Opaque token; resolved to a real phone number only inside the provider adapter.
    phone_token: Mapped[str] = mapped_column(String(255), nullable=False)

    state: Mapped[str] = mapped_column(String(20), nullable=False, default="ELIGIBLE")

    # How many dial attempts have been made for this campaign.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # The allocator will not select this borrower until this timestamp passes.
    next_eligible_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Higher priority borrowers are dialled first within a campaign.
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="borrower")


# --------------------------------------------------------------------------- #
# Call                                                                         #
# --------------------------------------------------------------------------- #

class Call(Base):
    """
    Central ledger row for one outbound call attempt.

    UUID primary key so callers can generate the ID before inserting —
    this is the idempotency key sent to the provider.

    `state_rank` mirrors the integer rank of the CallState enum (see
    domain/call_fsm.py).  An incoming event is only applied if its rank
    is strictly greater than the current rank, preventing out-of-order
    or replayed events from moving state backward.

    `lease_expires_at` is renewed by the event processor on each valid
    event.  The recovery sweeper reclaims any call whose lease has expired
    and is still in a non-terminal state.

    `reconcile_deadline` is set when a provider initiation returns TIMEOUT
    (call moves to UNKNOWN).  The sweeper must attempt reconciliation before
    this deadline; after it, the call is cancelled/failed with a note about
    unresolved ambiguity.
    """

    __tablename__ = "calls"
    __table_args__ = (
        CheckConstraint(
            "state IN ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','HELD',"
            "'CONNECTED','COMPLETED','FAILED','CANCELLED','UNKNOWN','ABANDONED')",
            name="ck_call_state",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    borrower_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("borrowers.id"), nullable=False
    )
    # Nullable: set when an agent is assigned.
    agent_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("agents.id"), nullable=True
    )

    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    # The call SID / leg ID returned by the provider after initiation.
    provider_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    state: Mapped[str] = mapped_column(String(20), nullable=False, default="QUEUED")

    # Integer rank matching CallState enum order.
    state_rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Optimistic-lock counter.
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Renewed on each valid event; sweeper acts when expired.
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Set when state=UNKNOWN (provider timeout).  Sweeper must reconcile before
    # this deadline or mark the call as permanently ambiguous.
    reconcile_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Timestamp when an answered call was put in HELD waiting for an agent.
    held_since: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Human-readable reason stored on terminal transitions.
    terminal_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    # Relationships
    borrower: Mapped["Borrower"] = relationship("Borrower", back_populates="calls")
    agent: Mapped["Agent | None"] = relationship("Agent", back_populates="calls")
    events: Mapped[list["CallEvent"]] = relationship("CallEvent", back_populates="call")


# --------------------------------------------------------------------------- #
# CallEvent  (events inbox)                                                    #
# --------------------------------------------------------------------------- #

class CallEvent(Base):
    """
    Inbox table for raw provider events.

    Every raw event is written here first (INSERT ... ON CONFLICT DO NOTHING
    using the UNIQUE(provider, event_id) constraint).  The event processor
    then picks up RECEIVED rows and applies them to the calls table.

    Deduplication contract
    ----------------------
    The UNIQUE(provider, event_id) index means concurrent ingest of the same
    event from multiple threads/processes results in exactly one row.  The
    losing INSERT is silently discarded (DO NOTHING).  This is the primary
    dedup mechanism — no application-level locking is needed for dedup.

    Status lifecycle
    ----------------
    RECEIVED       -> fresh row, not yet processed
    APPLIED        -> FSM transition was applied to the call row
    IGNORED_DUPLICATE -> event_id seen before (shouldn't reach processor, but
                        included for completeness if re-ingest races)
    IGNORED_LATE   -> call is in a terminal state; event arrived too late
    IGNORED_INVALID -> event type not recognised or call not found after retries
    QUARANTINED    -> call_id not found yet; will retry with exponential backoff
    DEAD_LETTER    -> max retries exceeded; event permanently parked

    `call_id` is nullable because an event may arrive before the call row is
    committed (race between provider callback and Txn 2 in the executor).
    The processor resolves call_id from provider_call_id and retries if missing.
    """

    __tablename__ = "call_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('RECEIVED','APPLIED','IGNORED_DUPLICATE','IGNORED_LATE',"
            "'IGNORED_INVALID','QUARANTINED','DEAD_LETTER')",
            name="ck_call_event_status",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Which provider emitted this event.
    provider: Mapped[str] = mapped_column(String(50), nullable=False)

    # Provider-supplied unique event identifier.  UNIQUE(provider, event_id)
    # enforced via partial index in schema.sql.
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Resolved after the call row exists.  Nullable to handle the race where
    # a provider callback beats the executor's Txn 2 commit.
    call_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("calls.id"), nullable=True, index=True
    )

    # The provider's own call leg identifier (used to look up the call row).
    provider_call_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Normalised event type (RINGING, ANSWERED, COMPLETED, FAILED, …).
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # Provider-supplied event timestamp for ordering/debugging.
    provider_ts: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # When we received this event.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    # Full raw payload for debugging/audit.
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    # Processing status.
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="RECEIVED"
    )

    # Human-readable reason for non-APPLIED statuses.
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    # How many processing attempts have been made.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # When to try again (for QUARANTINED rows with exponential backoff).
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Optional provider sequence number for ordered-delivery debugging.
    seq: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Relationship (nullable FK so use lazy load; call may not exist yet)
    call: Mapped["Call | None"] = relationship("Call", back_populates="events")


# --------------------------------------------------------------------------- #
# SafetyDecisionLog & SystemFlag (Phase 3)                                     #
# --------------------------------------------------------------------------- #

class SafetyDecisionLog(Base):
    """
    Audit log of every safety decision made by the safety controller.
    """

    __tablename__ = "safety_decisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(30), nullable=False)
    approved_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    rule_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    reasons: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class SystemFlag(Base):
    """
    Operator control table for system-wide flags, including kill switches.
    pacing/ must NEVER import or reference this table directly.
    """

    __tablename__ = "system_flags"

    flag_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    flag_value: Mapped[str] = mapped_column(String(255), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class PacingDecisionLog(Base):
    """
    Audit log of predictive pacing engine calculations and decisions.
    """

    __tablename__ = "pacing_decisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    requested_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    inflight: Mapped[int] = mapped_column(Integer, nullable=False)
    p_hi: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    max_calls: Mapped[int] = mapped_column(Integer, nullable=False)
    formula: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


# --------------------------------------------------------------------------- #
# Phase 8: Dashboard Models                                                   #
# --------------------------------------------------------------------------- #

class OperatorActionLog(Base):
    """
    Audit log of all control actions executed via the operator dashboard API.
    """
    __tablename__ = "operator_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    operator_token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class MetricsSnapshot(Base):
    """
    Periodic timeseries bucket snapshot for fast dashboard rendering.
    """
    __tablename__ = "metrics_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    in_flight: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    connected: Mapped[int] = mapped_column(Integer, nullable=False)
    abandoned: Mapped[int] = mapped_column(Integer, nullable=False)
    utilization_pct: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    p_mean: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    p_hi: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


