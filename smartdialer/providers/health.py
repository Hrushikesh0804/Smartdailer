"""
providers/health.py — Rolling failure rate, latency tracking, and circuit breaker.

Architecture
------------
ProviderHealth tracks a time-windowed sliding window of (timestamp, success, latency_ms)
tuples.  The window is trimmed on every record() call so memory is bounded.

CircuitBreaker wraps ProviderHealth and implements the standard three-state machine:
  CLOSED   -> normal operation; all calls go through.
  OPEN     -> failure rate exceeded threshold; all calls fail fast (no provider call).
  HALF_OPEN -> cooldown elapsed; one probe call is allowed to test recovery.

Why a circuit breaker?
  Without it, a degraded provider would pile up TIMEOUT calls, exhaust agent
  reservations, and degrade answer rates.  The circuit breaker fails fast,
  releases resources immediately, and allows controlled recovery probes.

Thresholds are read from settings so they can be tuned per-deployment.

Threading
---------
A threading.Lock protects the window list and state transitions.  All methods
are safe to call from multiple worker threads.
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timedelta
from enum import Enum

from smartdialer.clock import Clock, SystemClock
from smartdialer.config import settings


class CircuitState(str, Enum):
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class ProviderHealth:
    """
    Time-windowed rolling statistics for one provider.

    Records (timestamp, success, latency_ms) tuples.
    Provides failure_rate() and avg_latency_ms() over the configured window.
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        # Each entry: (timestamp, is_success, latency_ms)
        self._window: deque[tuple[datetime, bool, float]] = deque()
        self._lock = threading.Lock()

    def record(self, success: bool, latency_ms: float) -> None:
        """Record one call outcome."""
        now = self._clock.now()
        cutoff = now - timedelta(seconds=settings.health_window_seconds)
        with self._lock:
            self._window.append((now, success, latency_ms))
            # Trim entries outside the window.
            while self._window and self._window[0][0] < cutoff:
                self._window.popleft()

    def _trim(self) -> list[tuple[datetime, bool, float]]:
        """Return current window entries after trimming stale ones."""
        now = self._clock.now()
        cutoff = now - timedelta(seconds=settings.health_window_seconds)
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
        return list(self._window)

    def failure_rate(self) -> float:
        """Fraction of calls in the window that failed.  0.0 if window is empty."""
        with self._lock:
            entries = self._trim()
        if not entries:
            return 0.0
        failures = sum(1 for _, ok, _ in entries if not ok)
        return failures / len(entries)

    def avg_latency_ms(self) -> float:
        """Average latency in the window.  0.0 if window is empty."""
        with self._lock:
            entries = self._trim()
        if not entries:
            return 0.0
        return sum(lat for _, _, lat in entries) / len(entries)

    def call_count(self) -> int:
        """Number of calls in the current window."""
        with self._lock:
            entries = self._trim()
        return len(entries)

    def snapshot(self) -> dict:
        """Return a dict snapshot for structured logging."""
        return {
            "call_count": self.call_count(),
            "failure_rate": round(self.failure_rate(), 4),
            "avg_latency_ms": round(self.avg_latency_ms(), 1),
        }


class CircuitBreaker:
    """
    Standard three-state circuit breaker wrapping a ProviderHealth tracker.

    State transitions
    -----------------
    CLOSED -> OPEN:
      failure_rate > threshold AND call_count >= min_calls.
    OPEN -> HALF_OPEN:
      cooldown elapsed (now >= open_since + cooldown_seconds).
    HALF_OPEN -> CLOSED:
      probe call succeeded.
    HALF_OPEN -> OPEN:
      probe call failed.

    Usage
    -----
        if not breaker.allow_call():
            return fail_fast()
        result = provider.initiate_call(...)
        breaker.record(success=(result.status == ACCEPTED), latency_ms=...)
    """

    def __init__(
        self,
        provider_name: str,
        clock: Clock | None = None,
    ) -> None:
        self._provider_name = provider_name
        self._clock = clock or SystemClock()
        self._health = ProviderHealth(clock=self._clock)
        self._state = CircuitState.CLOSED
        self._open_since: datetime | None = None
        self._probe_count: int = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def health(self) -> ProviderHealth:
        return self._health

    def allow_call(self) -> bool:
        """
        Return True if a call should be attempted.

        Side effect: may transition OPEN -> HALF_OPEN if the cooldown has elapsed.
        """
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                now = self._clock.now()
                cooldown = timedelta(seconds=settings.circuit_breaker_cooldown_seconds)
                if self._open_since and now >= self._open_since + cooldown:
                    self._state = CircuitState.HALF_OPEN
                    self._probe_count = 1
                    return True  # allow first probe
                return False

            if self._state == CircuitState.HALF_OPEN:
                if self._probe_count < settings.circuit_breaker_probe_limit:
                    self._probe_count += 1
                    return True
                return False

        return False  # unreachable

    def record(self, success: bool, latency_ms: float) -> None:
        """
        Record a call outcome and potentially change circuit state.

        Must be called after every provider call that was allowed through.
        """
        self._health.record(success=success, latency_ms=latency_ms)

        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                if success:
                    self._state = CircuitState.CLOSED
                    self._open_since = None
                else:
                    self._state = CircuitState.OPEN
                    self._open_since = self._clock.now()
                return

            if self._state == CircuitState.CLOSED:
                count = self._health.call_count()
                rate = self._health.failure_rate()
                if (
                    count >= settings.circuit_breaker_min_calls
                    and rate > settings.circuit_breaker_failure_threshold
                ):
                    self._state = CircuitState.OPEN
                    self._open_since = self._clock.now()

    def snapshot(self) -> dict:
        """Structured snapshot for logging."""
        return {
            "provider": self._provider_name,
            "circuit_state": self._state.value,
            "health": self._health.snapshot(),
        }
