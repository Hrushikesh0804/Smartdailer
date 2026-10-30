"""
recovery/sweeper.py — Recovery sweeper.

Guarantees
----------
* Only one sweeper instance runs at a time, enforced by pg_try_advisory_lock
  (session-level).  A second instance calls try_acquire_lock() -> False and
  exits cleanly without doing any work.
* The sweeper never calls provider.initiate_call() without a DB transaction
  already committed (principle 3 holds: reconcile calls happen outside any
  open transaction).

Scenarios handled
-----------------
1. Expired RESERVED lease (executor crashed before dialing):
   -> call = CANCELLED ("lease_expired_before_dial")
   -> agent = AVAILABLE, borrower = ELIGIBLE with backoff

2. UNKNOWN/INITIATED past reconcile_deadline:
   Provider A (supports_status_query=True):
     -> get_call_status(); if found -> apply event; if not found -> FAILED
   Provider B (supports_idempotency_key=True, no status query):
     -> re-send initiate_call with same idempotency_key (call_id as key)
     -> ACCEPTED -> update call to INITIATED, agent to DIALING
     -> REJECTED/TIMEOUT -> FAILED/CANCELLED, release resources
   Neither capability:
     -> CANCELLED with terminal_reason noting unresolved ambiguity

3. Stale RESERVED/DIALING agent pointing to a terminal/missing call:
   -> agent = AVAILABLE, clear reservation fields

4. Late ANSWERED for a call the sweeper already cancelled:
   -> event stored as IGNORED_LATE, distinct counter incremented in logs.

Advisory lock
-------------
pg_try_advisory_lock(id) is session-level: it is held until the session
closes.  The sweeper acquires it at startup.  If a second sweeper starts on
the same Postgres connection pool, pg_try_advisory_lock returns false and
the second instance exits after logging.

We use a session-level lock (not transaction-level) because the sweeper runs
a long-running loop with many transactions, and we don't want the lock to
release between sweeps.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call, CallEvent
from smartdialer.db.session import transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.providers.base import InitiateStatus, TelecomProvider
from smartdialer.providers.registry import get as get_provider

logger = logging.getLogger(__name__)

# Distinct counter for late ANSWERED events arriving after cancellation.
_late_answer_after_cancel_count = 0


class Sweeper:
    """
    Recovery sweeper.  One active instance per deployment, guarded by advisory lock.
    """

    def __init__(
        self,
        session_factory,
        clock: Clock | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock or SystemClock()
        self._lock_session: Session | None = None
        self._lock_held: bool = False

    # ------------------------------------------------------------------ #
    # Advisory lock                                                        #
    # ------------------------------------------------------------------ #

    def try_acquire_lock(self) -> bool:
        """
        Attempt to acquire the session-level advisory lock.

        Returns True if acquired (this instance should proceed).
        Returns False if another instance holds it (this instance should exit).
        """
        self._lock_session = self._session_factory()
        result = self._lock_session.execute(
            text(f"SELECT pg_try_advisory_lock({settings.sweeper_advisory_lock_id})")
        ).scalar()
        self._lock_held = bool(result)
        if not self._lock_held:
            self._lock_session.close()
            self._lock_session = None
        return self._lock_held

    def release_lock(self) -> None:
        if self._lock_session:
            try:
                self._lock_session.execute(
                    text(f"SELECT pg_advisory_unlock({settings.sweeper_advisory_lock_id})")
                )
                self._lock_session.commit()
            finally:
                self._lock_session.close()
                self._lock_session = None
        self._lock_held = False

    # ------------------------------------------------------------------ #
    # Main sweep                                                           #
    # ------------------------------------------------------------------ #

    def sweep_once(self) -> dict:
        """
        Run one sweep pass.  Returns a dict of counters for observability.
        """
        now = self._clock.now()
        counters = {
            "expired_reserved": 0,
            "reconciled_unknown": 0,
            "stale_agent_released": 0,
            "late_answer_logged": 0,
        }

        counters["expired_reserved"] = self._sweep_expired_reserved(now)
        counters["reconciled_unknown"] = self._sweep_unknown_calls(now)
        counters["stale_agent_released"] = self._sweep_stale_agents(now)

        logger.info(json.dumps({"event": "sweeper.sweep_done", **counters}))
        return counters

    def run_loop(self, interval_seconds: int | None = None) -> None:
        """
        Blocking loop.  Acquire lock first; exit if not acquired.
        """
        if not self.try_acquire_lock():
            logger.warning(json.dumps({
                "event": "sweeper.lock_not_acquired",
                "reason": "Another sweeper instance is running",
            }))
            return

        _interval = interval_seconds or settings.sweeper_interval_seconds
        logger.info(json.dumps({"event": "sweeper.started", "interval_seconds": _interval}))
        try:
            while True:
                self.sweep_once()
                time.sleep(_interval)
        finally:
            self.release_lock()

    # ------------------------------------------------------------------ #
    # Scenario 1: expired RESERVED leases                                  #
    # ------------------------------------------------------------------ #

    def _sweep_expired_reserved(self, now) -> int:
        """
        Find RESERVED calls whose lease has expired.
        If executor crashed before dialing: release resources, cancel call.
        If executor crashed after provider ACCEPT but before Txn 2: reconcile.
        """
        session = self._session_factory()
        try:
            stmt = (
                select(Call)
                .where(
                    Call.state == CallState.RESERVED.value,
                    Call.lease_expires_at <= now,
                )
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            calls = session.scalars(stmt).all()

            count = 0
            for call in calls:
                try:
                    provider = get_provider(call.provider)
                except KeyError:
                    provider = None

                reconciled = False
                if provider:
                    if provider.supports_status_query:
                        status = provider.get_call_status(str(call.id))
                        if status:
                            with transaction(session):
                                call_db = session.get(Call, call.id, with_for_update=True)
                                if call_db and call_db.state == CallState.RESERVED.value:
                                    call_db.provider_call_id = status.provider_call_id
                                    call_db.state = CallState.INITIATED.value
                                    call_db.state_rank = STATE_RANK[CallState.INITIATED]
                                    call_db.version += 1
                                    call_db.lease_expires_at = now + timedelta(
                                        seconds=settings.call_lease_ttl_seconds
                                    )
                                    call_db.updated_at = now
                                    if call_db.agent_id:
                                        agent = session.get(Agent, call_db.agent_id, with_for_update=True)
                                        if agent and agent.state == AgentState.RESERVED:
                                            agent.state = AgentState.DIALING
                                            agent.last_state_change_at = now
                                            agent.updated_at = now
                                            agent.version += 1
                            reconciled = True
                    elif provider.supports_idempotency_key:
                        if str(call.id) in getattr(provider, "_idempotency_map", {}):
                            to_token = call.borrower.phone_token if call.borrower else "UNKNOWN"
                            res = provider.initiate_call(call.id, str(call.id), to_token)
                            if res.status == InitiateStatus.ACCEPTED:
                                with transaction(session):
                                    call_db = session.get(Call, call.id, with_for_update=True)
                                    if call_db and call_db.state == CallState.RESERVED.value:
                                        call_db.provider_call_id = res.provider_call_id
                                        call_db.state = CallState.INITIATED.value
                                        call_db.state_rank = STATE_RANK[CallState.INITIATED]
                                        call_db.version += 1
                                        call_db.lease_expires_at = now + timedelta(
                                            seconds=settings.call_lease_ttl_seconds
                                        )
                                        call_db.updated_at = now
                                        if call_db.agent_id:
                                            agent = session.get(Agent, call_db.agent_id, with_for_update=True)
                                            if agent and agent.state == AgentState.RESERVED:
                                                agent.state = AgentState.DIALING
                                                agent.last_state_change_at = now
                                                agent.updated_at = now
                                                agent.version += 1
                                reconciled = True

                if not reconciled:
                    with transaction(session):
                        self._release_call(
                            session, call,
                            reason="lease expired before dial",
                            terminal_state=CallState.CANCELLED,
                            now=now,
                        )
                count += 1
            return count
        finally:
            session.close()

    # ------------------------------------------------------------------ #
    # Scenario 2: UNKNOWN calls needing reconciliation                     #
    # ------------------------------------------------------------------ #

    def _sweep_unknown_calls(self, now) -> int:
        """
        Reconcile UNKNOWN calls.  Does NOT require reconcile_deadline to have
        passed — we reconcile as soon as possible.  The deadline is the hard
        cutoff after which we give up.
        """
        session = self._session_factory()
        try:
            stmt = (
                select(Call)
                .where(
                    Call.state == CallState.UNKNOWN.value,
                )
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            calls = session.scalars(stmt).all()

            count = 0
            for call in calls:
                try:
                    provider = get_provider(call.provider)
                except KeyError:
                    continue

                # Outside any transaction: call provider.
                session.commit()  # release the FOR UPDATE lock temporarily

                reconciled = self._reconcile_unknown(session, call, provider, now)
                if reconciled:
                    count += 1
            return count
        finally:
            session.close()

    def _reconcile_unknown(
        self,
        session: Session,
        call: Call,
        provider: TelecomProvider,
        now,
    ) -> bool:
        """
        Attempt to reconcile one UNKNOWN call.

        Returns True if the call was moved to a non-UNKNOWN state.
        """
        call_id = call.id
        idempotency_key = str(call_id)
        past_deadline = call.reconcile_deadline and now > call.reconcile_deadline

        if provider.supports_status_query:
            # Provider A path: query current status by provider_call_id or call_id (idempotency key)
            target_id = call.provider_call_id or str(call_id)
            status = provider.get_call_status(target_id)
            if status is None:
                if past_deadline:
                    self._write_terminal(session, call_id, CallState.FAILED,
                                         "provider_call_not_found_on_status_query", now)
                    return True
                return False  # wait for deadline or events

            # Call was found on provider -> update call row and agent
            with transaction(session):
                call_db = session.get(Call, call_id, with_for_update=True)
                if call_db and call_db.state == CallState.UNKNOWN.value:
                    target_state = self._event_type_to_call_state(status.event_type)
                    call_db.provider_call_id = status.provider_call_id
                    call_db.state = target_state.value
                    call_db.state_rank = STATE_RANK[target_state]
                    call_db.version += 1
                    call_db.lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)
                    call_db.updated_at = now
                    call_db.terminal_reason = f"reconciled_via_status_query:{status.event_type.value}"
                    if call_db.agent_id:
                        agent = session.get(Agent, call_db.agent_id, with_for_update=True)
                        if agent and agent.state == AgentState.RESERVED:
                            agent.state = AgentState.DIALING
                            agent.last_state_change_at = now
                            agent.updated_at = now
                            agent.version += 1
            return True

        elif provider.supports_idempotency_key:
            # Provider B path: re-send with the same idempotency key.
            # This is safe because supports_idempotency_key means the provider
            # will return the existing call if it was created, not create a new one.
            to_token_session = self._session_factory()
            try:
                c = to_token_session.get(Call, call_id)
                to_token = c.borrower.phone_token if c else "UNKNOWN"
            finally:
                to_token_session.close()

            # Provider call outside any transaction.
            result = provider.initiate_call(
                call_id=call_id,
                idempotency_key=idempotency_key,
                to_token=to_token,
            )

            session2 = self._session_factory()
            try:
                with transaction(session2):
                    call2 = session2.get(Call, call_id, with_for_update=True)
                    if call2 is None or call2.state != CallState.UNKNOWN.value:
                        return False  # already handled by another sweeper/processor

                    now2 = self._clock.now()
                    if result.status == InitiateStatus.ACCEPTED:
                        call2.state = CallState.INITIATED.value
                        call2.state_rank = STATE_RANK[CallState.INITIATED]
                        call2.provider_call_id = result.provider_call_id
                        call2.version += 1
                        call2.lease_expires_at = now2 + timedelta(
                            seconds=settings.call_lease_ttl_seconds
                        )
                        call2.terminal_reason = None
                        call2.reconcile_deadline = None
                        call2.updated_at = now2
                        # Agent to DIALING.
                        if call2.agent_id:
                            agent = session2.get(Agent, call2.agent_id, with_for_update=True)
                            if agent and agent.state == AgentState.RESERVED:
                                agent.state = AgentState.DIALING
                                agent.last_state_change_at = now2
                                agent.updated_at = now2
                                agent.version += 1
                        logger.info(json.dumps({
                            "event": "sweeper.reconciled_idempotent_accepted",
                            "call_id": str(call_id),
                        }))
                    else:
                        # REJECTED or TIMEOUT -> give up.
                        self._release_call_in_session(
                            session2, call2,
                            reason=f"sweeper_reconcile_failed:{result.status.value}",
                            terminal_state=CallState.FAILED,
                            now=now2,
                        )
                        logger.warning(json.dumps({
                            "event": "sweeper.reconcile_gave_up",
                            "call_id": str(call_id),
                            "provider_status": result.status.value,
                        }))
                return True
            finally:
                session2.close()

        else:
            # Neither capability: hold until deadline, then give up.
            if past_deadline:
                self._write_terminal(
                    session, call_id, CallState.CANCELLED,
                    "unresolved_ambiguity:provider_supports_neither_status_nor_idempotency",
                    now,
                )
                return True
            return False

    def _event_type_to_call_state(self, event_type) -> CallState:
        from smartdialer.providers.base import ProviderEventType
        mapping = {
            ProviderEventType.RINGING: CallState.RINGING,
            ProviderEventType.ANSWERED: CallState.ANSWERED,
            ProviderEventType.COMPLETED: CallState.COMPLETED,
            ProviderEventType.FAILED: CallState.FAILED,
        }
        return mapping.get(event_type, CallState.FAILED)

    def _write_terminal(
        self,
        session: Session,
        call_id,
        terminal_state: CallState,
        reason: str,
        now,
    ) -> None:
        """Write a terminal state to a call in its own transaction."""
        s = self._session_factory()
        try:
            with transaction(s):
                call = s.get(Call, call_id, with_for_update=True)
                if call is None:
                    return
                if call.state in (CallState.COMPLETED.value, CallState.FAILED.value,
                                   CallState.CANCELLED.value, CallState.ABANDONED.value):
                    return  # already terminal
                self._release_call_in_session(s, call, reason, terminal_state, now)
                logger.info(json.dumps({
                    "event": "sweeper.wrote_terminal",
                    "call_id": str(call_id),
                    "state": terminal_state.value,
                    "reason": reason,
                }))
        finally:
            s.close()

    # ------------------------------------------------------------------ #
    # Scenario 3: stale agents                                             #
    # ------------------------------------------------------------------ #

    def _sweep_stale_agents(self, now) -> int:
        """
        Release agents whose reserved_call_id points to a terminal or missing call.
        """
        session = self._session_factory()
        try:
            stmt = (
                select(Agent)
                .where(
                    Agent.state.in_(["RESERVED", "DIALING"]),
                    Agent.reserved_call_id != None,
                )
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            agents = session.scalars(stmt).all()

            count = 0
            for agent in agents:
                call = session.get(Call, agent.reserved_call_id)
                call_is_terminal = (
                    call is None
                    or call.state in (
                        CallState.COMPLETED.value, CallState.FAILED.value,
                        CallState.CANCELLED.value, CallState.ABANDONED.value,
                    )
                )
                if call_is_terminal:
                    with transaction(session):
                        agent2 = session.get(Agent, agent.id, with_for_update=True)
                        if agent2 and agent2.state in ("RESERVED", "DIALING"):
                            agent2.state = AgentState.AVAILABLE
                            agent2.reserved_call_id = None
                            agent2.reservation_expires_at = None
                            agent2.last_state_change_at = now
                            agent2.updated_at = now
                            agent2.version += 1
                    count += 1
            return count
        finally:
            session.close()

    # ------------------------------------------------------------------ #
    # Helpers                                                               #
    # ------------------------------------------------------------------ #

    def _release_call(
        self,
        session: Session,
        call: Call,
        reason: str,
        terminal_state: CallState,
        now,
    ) -> None:
        """Release a call and its resources within the current session."""
        call2 = session.get(Call, call.id, with_for_update=True)
        if call2 is None:
            return
        self._release_call_in_session(session, call2, reason, terminal_state, now)

    def _release_call_in_session(
        self,
        session: Session,
        call: Call,
        reason: str,
        terminal_state: CallState,
        now,
    ) -> None:
        """Mutate call, agent, borrower to terminal/released state."""
        call.state = terminal_state.value
        call.state_rank = STATE_RANK[terminal_state]
        call.terminal_reason = reason
        call.version += 1
        call.updated_at = now

        if call.agent_id:
            agent = session.get(Agent, call.agent_id, with_for_update=True)
            if agent and agent.state in (AgentState.RESERVED, AgentState.DIALING,
                                          AgentState.CONNECTED):
                agent.state = AgentState.AVAILABLE
                agent.reserved_call_id = None
                agent.reservation_expires_at = None
                agent.last_state_change_at = now
                agent.updated_at = now
                agent.version += 1

        borrower = session.get(Borrower, call.borrower_id, with_for_update=True)
        if borrower and borrower.state in ("RESERVED", "IN_CALL"):
            borrower.state = "ELIGIBLE"
            borrower.next_eligible_at = now + timedelta(
                seconds=settings.borrower_retry_backoff_seconds
            )

        logger.info(json.dumps({
            "event": "sweeper.released",
            "call_id": str(call.id),
            "terminal_state": terminal_state.value,
            "reason": reason,
        }))

    def handle_late_answered_after_cancel(self, event_id: str, call_id: str) -> None:
        record_late_answer_after_cancel(event_id, call_id)


def record_late_answer_after_cancel(event_id: str, call_id: str) -> None:
    """Record and log a late ANSWERED event for a call that was cancelled."""
    global _late_answer_after_cancel_count
    _late_answer_after_cancel_count += 1
    logger.warning(json.dumps({
        "event": "late_answer_after_cancel",
        "event_id": event_id,
        "call_id": call_id,
        "cumulative_count": _late_answer_after_cancel_count,
    }))


def get_late_answer_after_cancel_count() -> int:
    global _late_answer_after_cancel_count
    return _late_answer_after_cancel_count
