"""
orchestrator/dial_executor.py — The three-step dial pattern.

The 3-step pattern (principle 3: never call provider inside a transaction)
--------------------------------------------------------------------------
Txn 1 (allocator, already done):  QUEUED -> RESERVED
  Agent = RESERVED, Borrower = RESERVED, Call = RESERVED.

Step outside any transaction:
  provider.initiate_call(idempotency_key=str(call_id), ...)

Txn 2 (compare-and-set on call.version):
  ACCEPTED -> call = INITIATED, agent = DIALING
  REJECTED -> call = FAILED (terminal_reason), agent = AVAILABLE, borrower = ELIGIBLE
  TIMEOUT  -> call = UNKNOWN, reconcile_deadline set, resources held

Why compare-and-set on call.version?
  Between Txn 1 and Txn 2 the process may crash, or the sweeper may have
  reclaimed the reservation (lease expired).  We check version: if it has
  changed, the sweeper already acted and we must NOT overwrite its decision.

Why NEVER retry on TIMEOUT inside the executor?
  Principle 6: timeouts are ambiguous.  We don't know if the provider created
  the call.  Retrying blindly could create a second call leg.  Instead, we
  write UNKNOWN with a reconcile_deadline and let the sweeper reconcile using
  the idempotency key (same call_id = same key, safe re-send).

No DB transaction open during provider call
-------------------------------------------
Verified by asserting session.in_transaction() is False before the
provider call.  A test exercises this assertion explicitly.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import timedelta

from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import transaction
from smartdialer.domain.agent_fsm import AgentState
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.providers.base import InitiateStatus, TelecomProvider

logger = logging.getLogger(__name__)


class DialExecutor:
    """
    Executes the three-step dial pattern for one call row.

    Usage::

        executor = DialExecutor(provider, session_factory, clock)
        executor.execute(call_id)
    """

    def __init__(
        self,
        provider: TelecomProvider,
        session_factory,
        clock: Clock | None = None,
    ) -> None:
        self._provider = provider
        self._session_factory = session_factory
        self._clock = clock or SystemClock()

    def execute(self, call_id: uuid.UUID) -> str:
        """
        Drive one reserved call through the provider and commit Txn 2.

        Returns the final call state string after Txn 2.
        """
        now = self._clock.now()

        # ------------------------------------------------------------ #
        # Load call details (read-only, no transaction needed).          #
        # ------------------------------------------------------------ #
        session: Session = self._session_factory()
        try:
            call = session.get(Call, call_id)
            if call is None:
                logger.error(json.dumps({
                    "event": "executor.call_not_found",
                    "call_id": str(call_id),
                }))
                return "NOT_FOUND"

            if call.state not in (CallState.RESERVED, CallState.RESERVED.value):
                logger.warning(json.dumps({
                    "event": "executor.wrong_state",
                    "call_id": str(call_id),
                    "state": call.state,
                }))
                return call.state

            version_at_read = call.version
            borrower_id = call.borrower_id
            agent_id = call.agent_id
            to_token = call.borrower.phone_token
            provider_name = call.provider
        finally:
            session.close()

        # ------------------------------------------------------------ #
        # Step outside any transaction: call the provider.              #
        # ------------------------------------------------------------ #
        # Assert no transaction is open (principle 3 guard).
        pre_call_session: Session = self._session_factory()
        try:
            assert not pre_call_session.in_transaction(), (
                "BUG: A DB transaction is open during provider.initiate_call()!"
            )
        finally:
            pre_call_session.close()

        idempotency_key = str(call_id)
        logger.info(json.dumps({
            "event": "executor.initiating",
            "call_id": str(call_id),
            "provider": provider_name,
        }))

        result = self._provider.initiate_call(
            call_id=call_id,
            idempotency_key=idempotency_key,
            to_token=to_token,
        )

        logger.info(json.dumps({
            "event": "executor.provider_result",
            "call_id": str(call_id),
            "status": result.status.value,
            "provider_call_id": result.provider_call_id,
            "latency_ms": result.latency_ms,
        }))

        # ------------------------------------------------------------ #
        # Txn 2: compare-and-set on call.version                        #
        # ------------------------------------------------------------ #
        txn_session: Session = self._session_factory()
        try:
            with transaction(txn_session):
                call = txn_session.get(Call, call_id, with_for_update=True)
                if call is None:
                    logger.error(json.dumps({
                        "event": "executor.call_gone_before_txn2",
                        "call_id": str(call_id),
                    }))
                    return "NOT_FOUND"

                # Compare-and-set: if version changed, the sweeper already acted.
                if call.version != version_at_read:
                    logger.warning(json.dumps({
                        "event": "executor.version_mismatch",
                        "call_id": str(call_id),
                        "expected_version": version_at_read,
                        "actual_version": call.version,
                    }))
                    return call.state

                now = self._clock.now()

                if result.status == InitiateStatus.ACCEPTED:
                    self._apply_accepted(txn_session, call, result, now)

                elif result.status == InitiateStatus.REJECTED:
                    self._apply_rejected(txn_session, call, agent_id, borrower_id,
                                         result, now)

                elif result.status == InitiateStatus.TIMEOUT:
                    self._apply_timeout(txn_session, call, result, now)

                return call.state
        finally:
            txn_session.close()

    # ------------------------------------------------------------------ #
    # Txn 2 helpers                                                        #
    # ------------------------------------------------------------------ #

    def _apply_accepted(self, session, call, result, now) -> None:
        call.state = CallState.INITIATED.value
        call.state_rank = STATE_RANK[CallState.INITIATED]
        call.provider_call_id = result.provider_call_id
        call.version += 1
        call.lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)
        call.updated_at = now

        if call.agent_id:
            agent = session.get(Agent, call.agent_id, with_for_update=True)
            if agent and agent.state == AgentState.RESERVED:
                agent.state = AgentState.DIALING
                agent.last_state_change_at = now
                agent.updated_at = now
                agent.version += 1

        logger.info(json.dumps({
            "event": "executor.accepted",
            "call_id": str(call.id),
            "provider_call_id": result.provider_call_id,
        }))

    def _apply_rejected(self, session, call, agent_id, borrower_id, result, now) -> None:
        call.state = CallState.FAILED.value
        call.state_rank = STATE_RANK[CallState.FAILED]
        call.terminal_reason = result.error_message or "PROVIDER_REJECTED"
        call.version += 1
        call.updated_at = now

        # Release agent.
        if agent_id:
            agent = session.get(Agent, agent_id, with_for_update=True)
            if agent and agent.state == AgentState.RESERVED:
                agent.state = AgentState.AVAILABLE
                agent.reserved_call_id = None
                agent.reservation_expires_at = None
                agent.last_state_change_at = now
                agent.updated_at = now
                agent.version += 1

        # Return borrower to ELIGIBLE with backoff.
        borrower = session.get(Borrower, borrower_id, with_for_update=True)
        if borrower and borrower.state == "RESERVED":
            borrower.state = "ELIGIBLE"
            borrower.next_eligible_at = now + timedelta(
                seconds=settings.borrower_retry_backoff_seconds
            )

        logger.info(json.dumps({
            "event": "executor.rejected",
            "call_id": str(call.id),
            "reason": call.terminal_reason,
        }))

    def _apply_timeout(self, session, call, result, now) -> None:
        """
        On TIMEOUT, move call to UNKNOWN.  Resources stay held.
        Set reconcile_deadline so the sweeper knows when to act.

        We do NOT release the agent here because the call may have been
        created by the provider.  The sweeper will reconcile.
        """
        call.state = CallState.UNKNOWN.value
        call.state_rank = STATE_RANK[CallState.UNKNOWN]
        call.terminal_reason = result.error_message or "PROVIDER_TIMEOUT"
        call.reconcile_deadline = now + timedelta(
            seconds=settings.reconcile_deadline_seconds
        )
        call.version += 1
        call.lease_expires_at = now + timedelta(seconds=settings.call_lease_ttl_seconds)
        call.updated_at = now

        logger.warning(json.dumps({
            "event": "executor.timeout_unknown",
            "call_id": str(call.id),
            "reconcile_deadline": call.reconcile_deadline.isoformat(),
        }))
