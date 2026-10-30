"""
smartdialer.pacing.estimator — Beta-Bayesian answer-rate estimator.

Pure standard library math (no scipy, no numpy).

Mathematical Formulation
-------------------------
Prior : Beta(a0, b0) per campaign.
Posterior update with exponential decay:
  a <- a * decay + y
  b <- b * decay + (1 - y)
where y = 1 if human answered, 0 otherwise.

Posterior statistics:
  p_mean = a / (a + b)
  var    = (a * b) / ((a + b)^2 * (a + b + 1))
  p_hi   = clip(p_mean + z * sqrt(var), 0.0, 1.0)

Drift Guard
-----------
We maintain both a long-term window (slow decay) and a short-term window
(fast decay).
  p_hi = max(p_hi_long, p_hi_recent)
This guarantees:
- A sudden spike in answer rate (e.g. 10% -> 70%) raises p_hi immediately.
- A drop lowers p_hi only slowly.
- We strictly protect against UNDERestimating the answer rate (which would
  cause severe overdialing and regulatory abandonment spikes).

Staleness
---------
stale = True if:
  - total observed samples < MIN_SAMPLES, or
  - last observation update is older than STALE_SECONDS.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings


class Estimate(BaseModel):
    p_mean: float
    p_hi: float
    n_samples: int
    stale: bool
    source: str
    ring_time_avg: float
    talk_time_avg: float
    age_seconds: float = 0.0


class BetaDistribution:
    """Represents a Beta(alpha, beta) posterior."""

    def __init__(self, alpha: float, beta: float) -> None:
        self.alpha = max(0.001, alpha)
        self.beta = max(0.001, beta)

    def update(self, y: float, decay: float) -> None:
        self.alpha = self.alpha * decay + y
        self.beta = self.beta * decay + (1.0 - y)

    @property
    def mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)

    @property
    def variance(self) -> float:
        a, b = self.alpha, self.beta
        total = a + b
        return (a * b) / (total * total * (total + 1.0))

    def upper_bound(self, z: float) -> float:
        var = max(0.0, self.variance)
        std = math.sqrt(var)
        val = self.mean + z * std
        return max(0.0, min(1.0, val))


class CampaignEstimator:
    """Maintains dual-window Beta-Bayesian state for one campaign."""

    def __init__(
        self,
        campaign_id: int,
        a0: float,
        b0: float,
        clock: Clock | None = None,
    ) -> None:
        self.campaign_id = campaign_id
        self._clock = clock or SystemClock()
        self.long_window = BetaDistribution(a0, b0)
        self.recent_window = BetaDistribution(a0, b0)
        self.n_samples: int = 0
        self.last_update_at: datetime | None = None

        # Running duration averages (seconds)
        self.ring_time_avg: float = settings.expected_ring_time_seconds
        self.talk_time_avg: float = settings.expected_talk_time_seconds
        self._lock = threading.Lock()

    def record(
        self,
        answered: bool,
        ring_duration_s: float | None = None,
        talk_duration_s: float | None = None,
        now: datetime | None = None,
    ) -> None:
        _now = now or self._clock.now()
        y = 1.0 if answered else 0.0

        with self._lock:
            self.long_window.update(y, decay=settings.estimator_long_decay)
            self.recent_window.update(y, decay=settings.estimator_recent_decay)
            self.n_samples += 1
            self.last_update_at = _now

            # Exponential moving averages for timings
            if ring_duration_s is not None and ring_duration_s > 0:
                self.ring_time_avg = 0.9 * self.ring_time_avg + 0.1 * ring_duration_s
            if talk_duration_s is not None and talk_duration_s > 0:
                self.talk_time_avg = 0.9 * self.talk_time_avg + 0.1 * talk_duration_s

    def estimate(self, z: float | None = None, now: datetime | None = None) -> Estimate:
        _now = now or self._clock.now()
        _z = z if z is not None else settings.estimator_z_score

        with self._lock:
            p_hi_long = self.long_window.upper_bound(_z)
            p_hi_recent = self.recent_window.upper_bound(_z)

            # Drift guard: strictly take max of long and recent
            p_hi = max(p_hi_long, p_hi_recent)
            p_mean = self.recent_window.mean

            # Staleness check
            age_seconds = 0.0
            if self.last_update_at is not None:
                age_seconds = max(0.0, (_now - self.last_update_at).total_seconds())
            else:
                age_seconds = 999999.0

            stale = (
                self.n_samples < settings.min_estimate_samples
                or age_seconds > settings.max_estimate_age_seconds
            )

            return Estimate(
                p_mean=round(p_mean, 4),
                p_hi=round(p_hi, 4),
                n_samples=self.n_samples,
                stale=stale,
                source=f"Beta({self.recent_window.alpha:.2f},{self.recent_window.beta:.2f})",
                ring_time_avg=round(self.ring_time_avg, 2),
                talk_time_avg=round(self.talk_time_avg, 2),
                age_seconds=round(age_seconds, 1),
            )


# --------------------------------------------------------------------------- #
# Module-level registry & public API                                          #
# --------------------------------------------------------------------------- #

_estimators: dict[int, CampaignEstimator] = {}
_seeded_priors: dict[int, tuple[float, float]] = {}
_registry_lock = threading.Lock()


logger = logging.getLogger(__name__)


def load_priors_from_json(json_path: str | None = None) -> None:
    """Load pre-computed campaign priors from JSON file if available."""
    path = json_path or settings.priors_file_path
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            with _registry_lock:
                for k, v in data.items():
                    try:
                        cid = int(k)
                        _seeded_priors[cid] = (float(v["alpha"]), float(v["beta"]))
                    except (ValueError, KeyError):
                        continue
    except Exception as e:
        logger.warning(f"Failed to load priors from '{path}': {e}")


def get_or_create_estimator(
    campaign_id: int,
    clock: Clock | None = None,
) -> CampaignEstimator:
    with _registry_lock:
        if campaign_id not in _estimators:
            a0, b0 = _seeded_priors.get(
                campaign_id,
                (settings.beta_prior_alpha, settings.beta_prior_beta),
            )
            _estimators[campaign_id] = CampaignEstimator(
                campaign_id=campaign_id,
                a0=a0,
                b0=b0,
                clock=clock,
            )
        return _estimators[campaign_id]


def predict_answer_rate(
    campaign_id: int,
    provider: str | None = None,
    clock: Clock | None = None,
) -> Estimate:
    """
    Stable interface: predict answer rate for campaign.
    """
    est = get_or_create_estimator(campaign_id, clock=clock)
    _now = clock.now() if clock else SystemClock().now()
    return est.estimate(now=_now)


def record_dial_outcome(
    campaign_id: int,
    answered: bool,
    ring_duration_s: float | None = None,
    talk_duration_s: float | None = None,
    clock: Clock | None = None,
) -> None:
    """Record an observed outcome to update the posterior."""
    est = get_or_create_estimator(campaign_id, clock=clock)
    _now = clock.now() if clock else SystemClock().now()
    est.record(
        answered=answered,
        ring_duration_s=ring_duration_s,
        talk_duration_s=talk_duration_s,
        now=_now,
    )


def reset_estimator(campaign_id: int | None = None) -> None:
    """Reset estimators (used in test fixtures)."""
    with _registry_lock:
        if campaign_id is not None:
            _estimators.pop(campaign_id, None)
        else:
            _estimators.clear()


# Initialize priors on import if file exists
load_priors_from_json()
