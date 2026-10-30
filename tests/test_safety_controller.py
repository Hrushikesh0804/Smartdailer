"""
tests/test_safety_controller.py — Unit tests for Safety Controller rules R1 to R6.
"""

from datetime import datetime, timezone
import pytest

from smartdialer.config import settings
from smartdialer.safety.controller import (
    PacingMode,
    PacingRequest,
    SafetyAction,
    SafetyDecision,
    StateSnapshot,
    advise,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 1


def _make_snapshot(
    free_agents=5,
    in_flight=0,
    abandonment_rate=0.0,
    kill_switch=False,
    circuit_state="CLOSED",
    in_cooldown=False,
):
    return StateSnapshot(
        campaign_id=CAMPAIGN_ID,
        free_agents=free_agents,
        in_flight=in_flight,
        rolling_abandonment_rate=abandonment_rate,
        kill_switch_active=kill_switch,
        in_abandonment_cooldown=in_cooldown,
        provider_health={"mock_a": {"circuit_state": circuit_state, "failure_rate": 0.0}},
        selected_provider="mock_a",
    )


def test_rule_r6_kill_switch_rejects_immediately():
    """R6: Operator kill switch active -> immediate REJECT with approved=0."""
    snap = _make_snapshot(free_agents=10, kill_switch=True)
    req = PacingRequest(campaign_id=CAMPAIGN_ID, mode=PacingMode.PROGRESSIVE, requested_calls=5)

    decision = advise(snap, req)
    assert decision.action == SafetyAction.REJECT
    assert decision.approved_calls == 0
    assert "R6" in decision.rule_ids


def test_rule_r4_provider_circuit_open_rejects():
    """R4: Selected provider circuit breaker OPEN -> immediate REJECT with approved=0."""
    snap = _make_snapshot(free_agents=10, circuit_state="OPEN")
    req = PacingRequest(campaign_id=CAMPAIGN_ID, mode=PacingMode.PROGRESSIVE, requested_calls=5)

    decision = advise(snap, req)
    assert decision.action == SafetyAction.REJECT
    assert decision.approved_calls == 0
    assert "R4" in decision.rule_ids


def test_rule_r1_progressive_hard_cap():
    """R1: In progressive mode, approved calls cannot exceed available agents."""
    snap = _make_snapshot(free_agents=3)
    req = PacingRequest(campaign_id=CAMPAIGN_ID, mode=PacingMode.PROGRESSIVE, requested_calls=10)

    decision = advise(snap, req)
    assert decision.action == SafetyAction.REDUCE
    assert decision.approved_calls == 3
    assert "R1" in decision.rule_ids

    # When 0 agents available
    snap_zero = _make_snapshot(free_agents=0)
    decision_zero = advise(snap_zero, req)
    assert decision_zero.action == SafetyAction.REJECT
    assert decision_zero.approved_calls == 0


def test_rule_r2_predictive_overdial_cap():
    """R2: In predictive mode, in_flight + approved <= capacity * MAX_OVERDIAL_RATIO."""
    # capacity = 4, max_overdial = 3.0 -> max_concurrent = 12
    # current in_flight = 8 -> remaining headroom = 4
    snap = _make_snapshot(free_agents=4, in_flight=8)
    req = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=10,
        estimate_meta={"sample_count": 100, "age_seconds": 10},
    )

    decision = advise(snap, req)
    assert decision.action == SafetyAction.REDUCE
    assert decision.approved_calls == 4
    assert "R2" in decision.rule_ids


def test_rule_r2_absurdly_optimistic_estimate_cannot_exceed_cap():
    """
    Pacing request with an absurdly optimistic proposal (e.g. 1000 calls)
    is strictly hard-capped by R2 overdial ratio.
    """
    snap = _make_snapshot(free_agents=5, in_flight=0)
    # cap = 5 * 3.0 = 15
    req = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=1000,
        estimate_meta={"sample_count": 500, "age_seconds": 5},
    )

    decision = advise(snap, req)
    assert decision.approved_calls == 15
    assert decision.action == SafetyAction.REDUCE


def test_rule_r3_abandonment_exceeded_fallback_progressive():
    """R3: Abandonment rate > budget -> FALLBACK_PROGRESSIVE."""
    # budget is 0.03 (3%); rate is 0.04 (4%)
    snap = _make_snapshot(free_agents=5, abandonment_rate=0.04)
    req = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=10,
        estimate_meta={"sample_count": 100, "age_seconds": 10},
    )

    decision = advise(snap, req)
    assert decision.action == SafetyAction.FALLBACK_PROGRESSIVE
    # In fallback progressive mode, cap is free_agents (5)
    assert decision.approved_calls == 5
    assert "R3" in decision.rule_ids


def test_rule_r3_abandonment_hysteresis():
    """
    Hysteresis: After tripping fallback, cannot re-enter predictive unless
    rate < budget * 0.5 (0.015) during cooldown window.
    """
    # rate is 0.02 (below budget 0.03, but above recovery threshold 0.015) while in cooldown
    snap = _make_snapshot(free_agents=5, abandonment_rate=0.02, in_cooldown=True)
    req = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=10,
        estimate_meta={"sample_count": 100, "age_seconds": 10},
    )

    decision = advise(snap, req)
    assert decision.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert "R3" in decision.rule_ids

    # Now rate drops to 0.010 (< 0.015) while in cooldown -> allowed back to predictive!
    snap_recovered = _make_snapshot(free_agents=5, abandonment_rate=0.010, in_cooldown=True)
    decision_recovered = advise(snap_recovered, req)
    assert decision_recovered.action in (SafetyAction.APPROVE, SafetyAction.REDUCE)
    assert "R2" in decision_recovered.rule_ids


def test_rule_r5_estimate_quality_low_sample_or_stale():
    """R5: Missing, stale, or low sample estimate -> FALLBACK_PROGRESSIVE."""
    snap = _make_snapshot(free_agents=5)

    # 1. Missing estimate_meta
    req_missing = PacingRequest(campaign_id=CAMPAIGN_ID, mode=PacingMode.PREDICTIVE, requested_calls=5)
    dec_missing = advise(snap, req_missing)
    assert dec_missing.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert "R5" in dec_missing.rule_ids

    # 2. Low sample count (< 20)
    req_low = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=5,
        estimate_meta={"sample_count": 10, "age_seconds": 10},
    )
    dec_low = advise(snap, req_low)
    assert dec_low.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert "R5" in dec_low.rule_ids

    # 3. Stale estimate (> 300s)
    req_stale = PacingRequest(
        campaign_id=CAMPAIGN_ID,
        mode=PacingMode.PREDICTIVE,
        requested_calls=5,
        estimate_meta={"sample_count": 50, "age_seconds": 500},
    )
    dec_stale = advise(snap, req_stale)
    assert dec_stale.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert "R5" in dec_stale.rule_ids
