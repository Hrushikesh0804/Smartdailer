"""
tests/test_estimator.py — Tests for Beta-Bayesian answer-rate estimator.
"""

import random
from datetime import datetime, timezone
import pytest

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.pacing.estimator import (
    CampaignEstimator,
    predict_answer_rate,
    record_dial_outcome,
    reset_estimator,
)

NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CAMPAIGN_ID = 99


@pytest.fixture(autouse=True)
def clean_estimator():
    reset_estimator()
    yield
    reset_estimator()


def test_estimator_converges_to_true_rate():
    """Estimator converges to the true answer rate on a seeded stream."""
    clock = FakeClock(NOW)
    rng = random.Random(42)
    true_rate = 0.35

    est = CampaignEstimator(campaign_id=CAMPAIGN_ID, a0=1.0, b0=1.0, clock=clock)

    for i in range(300):
        answered = rng.random() < true_rate
        clock.advance(seconds=1)
        est.record(answered=answered)

    estimate = est.estimate()
    assert abs(estimate.p_mean - true_rate) < 0.08
    assert estimate.n_samples == 300
    assert estimate.stale is False


def test_drift_guard_sudden_spike_vs_slow_drop():
    """
    After a 10% -> 70% jump, p_hi exceeds 0.6 within a bounded number of samples.
    A drop back to 10% lowers p_hi only slowly.
    """
    clock = FakeClock(NOW)
    rng = random.Random(123)

    est = CampaignEstimator(campaign_id=CAMPAIGN_ID, a0=1.0, b0=1.0, clock=clock)

    # Establish baseline at ~10%
    for _ in range(50):
        clock.advance(seconds=1)
        est.record(answered=(rng.random() < 0.10))

    initial_est = est.estimate()
    assert initial_est.p_mean < 0.25

    # Sudden jump to 70%: within 25 samples, p_hi must exceed 0.6
    for _ in range(25):
        clock.advance(seconds=1)
        est.record(answered=(rng.random() < 0.70))

    jump_est = est.estimate()
    assert jump_est.p_hi >= 0.55, f"p_hi did not jump quickly enough: {jump_est.p_hi}"

    # Now drop back to 0% for 15 samples: p_hi should decay slowly due to long window
    high_phi = jump_est.p_hi
    for _ in range(15):
        clock.advance(seconds=1)
        est.record(answered=False)

    drop_est = est.estimate()
    # It should still remain elevated compared to pure 0% because long window holds it
    assert drop_est.p_hi > 0.30
    assert drop_est.p_hi <= high_phi


def test_estimator_staleness_and_sample_count():
    """Estimator is marked stale if samples < min_samples or no updates recently."""
    clock = FakeClock(NOW)
    est = CampaignEstimator(campaign_id=CAMPAIGN_ID, a0=1.0, b0=1.0, clock=clock)

    # 1. Stale due to low sample count
    for _ in range(settings.min_estimate_samples - 1):
        clock.advance(seconds=1)
        est.record(answered=True)

    assert est.estimate().stale is True

    # Reach min samples -> not stale
    clock.advance(seconds=1)
    est.record(answered=True)
    assert est.estimate().stale is False

    # 2. Stale due to time passing with no new updates
    clock.advance(seconds=settings.max_estimate_age_seconds + 10)
    assert est.estimate().stale is True
