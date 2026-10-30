"""
tests/test_predictive_unit.py — Unit tests and property-based tests for predictive pacing.
"""

import math
import random
import pytest
from hypothesis import given, settings as hyp_settings, strategies as st

from smartdialer.pacing.estimator import Estimate
from smartdialer.pacing.predictive import (
    binomial_tail_probability,
    calculate_predictive_pacing,
    find_max_calls_for_capacity,
)


def test_hand_computed_binomial_case():
    """
    Hand computed verification:
    N=3, c=1, p=0.5:
      P(X=0) = 0.125
      P(X=1) = 0.375
      P(X <= 1) = 0.5
      P(X > 1) = 0.5
    """
    tail_3 = binomial_tail_probability(n=3, k_threshold=1, p=0.5)
    assert abs(tail_3 - 0.5) < 1e-6

    # N=2, c=1, p=0.5:
    #   P(X <= 1) = P(X=0) + P(X=1) = 0.25 + 0.50 = 0.75
    #   P(X > 1) = 0.25
    tail_2 = binomial_tail_probability(n=2, k_threshold=1, p=0.5)
    assert abs(tail_2 - 0.25) < 1e-6

    # N=1, c=1, p=0.5:
    #   P(X > 1) = 0.0
    tail_1 = binomial_tail_probability(n=1, k_threshold=1, p=0.5)
    assert tail_1 == 0.0


def test_fixed_number_boundary_cases():
    """
    Boundary cases: capacity=0, p_hi=1.0, empty queue, huge capacity.
    """
    dummy_est = Estimate(
        p_mean=0.3,
        p_hi=0.4,
        n_samples=50,
        stale=False,
        source="test",
        ring_time_avg=5.0,
        talk_time_avg=60.0,
    )

    # 1. capacity = 0 -> requested = 0
    dec_cap0 = calculate_predictive_pacing(
        campaign_id=1, capacity=0, inflight=0, queue_size=10, estimate=dummy_est
    )
    assert dec_cap0.requested_calls == 0

    # 2. p_hi = 1.0 -> every call answered -> max_calls = capacity
    est_p1 = dummy_est.model_copy(update={"p_hi": 1.0, "p_mean": 1.0})
    dec_p1 = calculate_predictive_pacing(
        campaign_id=1, capacity=5, inflight=2, queue_size=10, estimate=est_p1
    )
    assert dec_p1.max_calls == 5
    assert dec_p1.requested_calls == 3  # capacity(5) - inflight(2)

    # 3. empty queue -> requested = 0
    dec_q0 = calculate_predictive_pacing(
        campaign_id=1, capacity=10, inflight=0, queue_size=0, estimate=dummy_est
    )
    assert dec_q0.requested_calls == 0

    # 4. huge capacity (e.g. 1000)
    dec_huge = calculate_predictive_pacing(
        campaign_id=1, capacity=500, inflight=0, queue_size=2000, estimate=dummy_est
    )
    assert dec_huge.requested_calls > 500  # overdialing allowed safely
    assert dec_huge.requested_calls <= 2000


@hyp_settings(max_examples=40, deadline=1000)
@given(
    p_true=st.floats(min_value=0.05, max_value=0.90),
    capacity=st.integers(min_value=1, max_value=100),
    p_margin=st.floats(min_value=0.0, max_value=0.08),
)
def test_hypothesis_binomial_bound_invariant(p_true: float, capacity: int, p_margin: float):
    """
    Hypothesis property test:
    For p_true in [0.05, 0.95], capacity in [1, 200], and p_hi >= p_true:
    The tail probability P(Binomial(N*, p_true) > capacity) stays <= EPSILON.
    """
    epsilon = 0.03
    p_hi = min(1.0, p_true + p_margin)

    n_star = find_max_calls_for_capacity(capacity=capacity, p_hi=p_hi, epsilon=epsilon)

    # Invariant check: with true p_true <= p_hi, tail under p_true must be <= epsilon
    true_tail = binomial_tail_probability(n=n_star, k_threshold=capacity, p=p_true)
    assert true_tail <= epsilon + 1e-5, (
        f"Binomial bound violated: n*={n_star}, cap={capacity}, p_true={p_true}, "
        f"p_hi={p_hi}, tail={true_tail} > {epsilon}"
    )
