"""
smartdialer.pacing.predictive — Predictive pacing engine.

Pure standard library math (no scipy, no numpy).

Mathematical Formulation
-------------------------
capacity = free_agents + agents_finishing_wrap_up_within_ring_time
inflight = INITIATED + RINGING calls
max_calls = largest N such that P(Binomial(N, p_hi) > capacity) <= EPSILON

Tail Computation:
  P(Binomial(N, p) <= c) = sum_{k=0}^{min(N, c)} math.comb(N, k) * (p^k) * (1-p)^(N-k)
  P(Binomial(N, p) > c)  = 1.0 - P(Binomial(N, p) <= c)

requested = max(0, min(max_calls - inflight, queue_size))
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings
from smartdialer.db.models import Agent, Borrower, Call, PacingDecisionLog
from smartdialer.domain.call_fsm import CallState
from smartdialer.pacing.estimator import Estimate, predict_answer_rate


class PacingDecision(BaseModel):
    campaign_id: int
    capacity: int
    inflight: int
    p_hi: float
    p_mean: float
    max_calls: int
    queue_size: int
    requested_calls: int
    formula: str
    why: str
    inputs: dict[str, Any] = {}


def binomial_tail_probability(n: int, k_threshold: int, p: float) -> float:
    """
    Compute P(Binomial(n, p) > k_threshold) using pure standard library math.
    Returns value in [0.0, 1.0].
    """
    if n <= k_threshold:
        return 0.0
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0 if n > k_threshold else 0.0

    # CDF: P(X <= k_threshold)
    cdf = 0.0
    q = 1.0 - p

    # Compute sum using log-space / math.comb
    for k in range(0, k_threshold + 1):
        # term = math.comb(n, k) * (p ** k) * (q ** (n - k))
        # Use log-space for numerical stability when n is moderately large
        log_term = (
            math.log(math.comb(n, k))
            + (k * math.log(p))
            + ((n - k) * math.log(q))
        )
        cdf += math.exp(log_term)

    tail = 1.0 - cdf
    return max(0.0, min(1.0, tail))


def find_max_calls_for_capacity(
    capacity: int,
    p_hi: float,
    epsilon: float | None = None,
    hard_bound: int | None = None,
) -> int:
    """
    Find largest integer N such that P(Binomial(N, p_hi) > capacity) <= epsilon.
    """
    _eps = epsilon if epsilon is not None else settings.binomial_epsilon

    if capacity <= 0:
        if p_hi <= 0.0:
            return hard_bound or 100
        # For capacity 0 and p > 0: P(X > 0) = 1 - (1-p)^N <= eps
        # If N=0: P(X > 0) = 0 <= eps
        # If N >= 1: 1 - (1-p)^N > eps (for typical small eps like 0.02)
        # Check if N=1 is allowed
        if (1.0 - (1.0 - p_hi)) <= _eps:
            return 1
        return 0

    if p_hi <= 0.0:
        return hard_bound or (capacity * 10)

    if p_hi >= 1.0:
        # Every call answered with probability 1. Can only launch up to capacity.
        return capacity

    # Search upwards from capacity
    max_search = hard_bound or max(capacity + 50, int(capacity * settings.max_overdial_ratio * 3))
    last_valid_n = capacity

    for n in range(capacity, max_search + 1):
        tail = binomial_tail_probability(n, capacity, p_hi)
        if tail <= _eps:
            last_valid_n = n
        else:
            # First N that violates the bound
            break

    return last_valid_n


def calculate_predictive_pacing(
    campaign_id: int,
    capacity: int,
    inflight: int,
    queue_size: int,
    estimate: Estimate,
    epsilon: float | None = None,
) -> PacingDecision:
    """
    Pure calculation of predictive dial request without DB access.
    """
    _eps = epsilon if epsilon is not None else settings.binomial_epsilon
    p_hi = estimate.p_hi
    p_mean = estimate.p_mean

    max_calls = find_max_calls_for_capacity(capacity, p_hi, epsilon=_eps)
    needed = max(0, max_calls - inflight)
    requested = min(needed, queue_size)

    formula = (
        f"max N s.t. P(Binomial(N, p_hi={p_hi}) > capacity={capacity}) <= {round(_eps, 3)}; "
        f"requested = min(N* - inflight={inflight}, queue_size={queue_size})"
    )

    why = (
        f"capacity={capacity}, p_hi={p_hi}, inflight={inflight}, "
        f"N*={max_calls}, requested={requested} "
        f"(limited by {'queue_size' if requested == queue_size and needed > queue_size else 'binomial bound'})"
    )

    return PacingDecision(
        campaign_id=campaign_id,
        capacity=capacity,
        inflight=inflight,
        p_hi=p_hi,
        p_mean=p_mean,
        max_calls=max_calls,
        queue_size=queue_size,
        requested_calls=requested,
        formula=formula,
        why=why,
        inputs={
            "capacity": capacity,
            "inflight": inflight,
            "queue_size": queue_size,
            "p_hi": p_hi,
            "p_mean": p_mean,
            "epsilon": _eps,
            "sample_count": estimate.n_samples,
            "n_samples": estimate.n_samples,
            "age_seconds": getattr(estimate, "age_seconds", 0.0),
            "stale": estimate.stale,
        },
    )


def compute_predictive_proposal(
    session: Session,
    campaign_id: int,
    clock: Clock | None = None,
    fixed_rate: float | None = None,
) -> PacingDecision:
    """
    Thin DB reader that fetches live counts, queries the estimator,
    computes the decision, and persists it into pacing_decisions.
    """
    _clock = clock or SystemClock()
    now = _clock.now()

    # 1. Available agents
    free_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "AVAILABLE",
        )
    ).scalar() or 0

    # 2. Wrap-up agents expected to finish within ring time
    # (For conservative estimation, we count wrap_up agents)
    wrap_up_agents = session.execute(
        select(func.count(Agent.id)).where(
            Agent.campaign_id == campaign_id,
            Agent.state == "WRAP_UP",
        )
    ).scalar() or 0

    # Capacity calculation
    capacity = free_agents + (wrap_up_agents // 2)

    # 3. Inflight calls (INITIATED + RINGING)
    inflight = session.execute(
        select(func.count(Call.id)).where(
            Call.campaign_id == campaign_id,
            Call.state.in_([CallState.INITIATED.value, CallState.RINGING.value]),
        )
    ).scalar() or 0

    # 4. Eligible borrowers queue size
    queue_size = session.execute(
        select(func.count(Borrower.id)).where(
            Borrower.campaign_id == campaign_id,
            Borrower.state == "ELIGIBLE",
            (Borrower.next_eligible_at == None) | (Borrower.next_eligible_at <= now),
        )
    ).scalar() or 0

    # 5. Query Beta-Bayesian estimator or use fixed historical rate
    if fixed_rate is not None:
        estimate = Estimate(
            p_mean=fixed_rate,
            p_hi=fixed_rate,
            n_samples=1000,
            stale=False,
            source="fixed_historical",
            ring_time_avg=settings.expected_ring_time_seconds,
            talk_time_avg=settings.expected_talk_time_seconds,
        )
    else:
        estimate = predict_answer_rate(campaign_id, clock=_clock)


    decision = calculate_predictive_pacing(
        campaign_id=campaign_id,
        capacity=capacity,
        inflight=inflight,
        queue_size=queue_size,
        estimate=estimate,
    )

    # 6. Persist to pacing_decisions table
    log_entry = PacingDecisionLog(
        campaign_id=campaign_id,
        requested_calls=decision.requested_calls,
        capacity=decision.capacity,
        inflight=decision.inflight,
        p_hi=decision.p_hi,
        max_calls=decision.max_calls,
        formula=decision.formula,
        reason=decision.why,
        inputs=decision.inputs,
        created_at=now,
    )
    session.add(log_entry)
    session.flush()

    return decision
