"""
tests/test_audit_bugs.py — Tests reproducing bugs discovered during hostile audit.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from smartdialer.clock import FakeClock
from smartdialer.db.models import Agent, Borrower, Call
from smartdialer.db.session import make_session_factory
from smartdialer.domain.call_fsm import CallState, STATE_RANK
from smartdialer.events.inbox import ingest
from smartdialer.events.processor import process_pending
from smartdialer.pacing.estimator import predict_answer_rate, reset_estimator
from smartdialer.providers.base import ProviderEvent, ProviderEventType

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 999


def test_event_processor_records_non_answer_on_failed_call(db_engine):
    """
    Bug 1: When a call fails without answering, the event processor must record
    a non-answer outcome (answered=False) to the Beta-Bayesian estimator so that
    the answer rate does not become artificially inflated.
    """
    session_factory = make_session_factory(db_engine)
    reset_estimator(CAMPAIGN_ID)

    # Initial state: 0 samples
    initial_est = predict_answer_rate(CAMPAIGN_ID)
    assert initial_est.n_samples == 0

    session = session_factory()
    try:
        b = Borrower(
            campaign_id=CAMPAIGN_ID,
            phone_token="tok_audit_fail",
            state="RESERVED",
            attempts=1,
        )
        session.add(b)
        session.flush()

        a = Agent(
            campaign_id=CAMPAIGN_ID,
            state="DIALING",
            last_state_change_at=NOW,
            updated_at=NOW,
        )
        session.add(a)
        session.flush()

        call_id = uuid.uuid4()
        c = Call(
            id=call_id,
            campaign_id=CAMPAIGN_ID,
            borrower_id=b.id,
            agent_id=a.id,
            provider="mock_a",
            provider_call_id="prov_audit_fail",
            state=CallState.INITIATED.value,
            state_rank=STATE_RANK[CallState.INITIATED],
            version=0,
            created_at=NOW,
            updated_at=NOW,
        )
        session.add(c)
        session.commit()
    finally:
        session.close()

    # Deliver FAILED event from provider
    clock = FakeClock(NOW)
    s = session_factory()
    try:
        ingest(
            session=s,
            provider_name="mock_a",
            event=ProviderEvent(
                event_id="evt_audit_failed_1",
                provider_call_id="prov_audit_fail",
                event_type=ProviderEventType.FAILED,
                provider_ts=NOW,
            ),
            clock=clock,
        )
    finally:
        s.close()

    # Process events
    processed = process_pending(session_factory, clock=clock)
    assert processed == 1

    # Verify estimator recorded the negative outcome
    updated_est = predict_answer_rate(CAMPAIGN_ID)
    assert updated_est.n_samples == 1, (
        f"Expected 1 recorded outcome on FAILED call, got {updated_est.n_samples}. "
        "The estimator was ignoring non-answering calls!"
    )


def test_safety_snapshot_with_invalid_provider_marks_circuit_open_not_closed(db_engine):
    """
    Bug 2: When an unknown or invalid provider is passed to get_state_snapshot,
    it must NOT silently declare the circuit CLOSED (healthy).
    It must mark the circuit OPEN so safety rejects calls to unknown providers.
    """
    from smartdialer.safety.controller import build_snapshot, advise, PacingRequest, PacingMode, SafetyAction

    session_factory = make_session_factory(db_engine)
    session = session_factory()
    try:
        snapshot = build_snapshot(session, campaign_id=1, selected_provider="invalid_carrier_99")
        health = snapshot.provider_health.get("invalid_carrier_99", {})
        assert health.get("circuit_state") == "OPEN", (
            f"Expected unknown provider circuit to be OPEN, but got {health.get('circuit_state')}. "
            "Safety would dangerously allow dials on a non-existent provider!"
        )
    finally:
        session.close()
