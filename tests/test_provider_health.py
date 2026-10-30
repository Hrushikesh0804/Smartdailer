"""
tests/test_provider_health.py — Unit tests for ProviderHealth and CircuitBreaker.
"""

from datetime import datetime, timezone
import pytest

from smartdialer.clock import FakeClock
from smartdialer.config import settings
from smartdialer.providers.health import CircuitBreaker, CircuitState, ProviderHealth


def test_provider_health_rolling_window():
    clock = FakeClock(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    health = ProviderHealth(clock=clock)

    health.record(success=True, latency_ms=100.0)
    health.record(success=False, latency_ms=200.0)

    assert health.call_count() == 2
    assert health.failure_rate() == 0.5
    assert health.avg_latency_ms() == 150.0

    # Advance beyond window (default window is 60s)
    clock.advance(seconds=settings.health_window_seconds + 5)
    # Stale items should be trimmed
    assert health.call_count() == 0
    assert health.failure_rate() == 0.0
    assert health.avg_latency_ms() == 0.0


def test_circuit_breaker_transitions():
    clock = FakeClock(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    cb = CircuitBreaker("mock_test", clock=clock)

    assert cb.state == CircuitState.CLOSED
    assert cb.allow_call() is True

    # Record failures below min_calls threshold
    for _ in range(settings.circuit_breaker_min_calls - 1):
        cb.record(success=False, latency_ms=100.0)
    assert cb.state == CircuitState.CLOSED

    # Meet min_calls and failure threshold (> 0.5)
    cb.record(success=False, latency_ms=100.0)
    assert cb.state == CircuitState.OPEN
    assert cb.allow_call() is False

    # Advance clock past cooldown
    clock.advance(seconds=settings.circuit_breaker_cooldown_seconds + 1)

    # First call transitions to HALF_OPEN and is allowed
    assert cb.allow_call() is True
    assert cb.state == CircuitState.HALF_OPEN

    # Exceed probe limit while in HALF_OPEN
    assert cb.allow_call() is False

    # Probe succeeds -> CLOSED
    cb.record(success=True, latency_ms=50.0)
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_call() is True


def test_circuit_breaker_half_open_failure_returns_to_open():
    clock = FakeClock(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    cb = CircuitBreaker("mock_test", clock=clock)

    # Force OPEN
    for _ in range(settings.circuit_breaker_min_calls):
        cb.record(success=False, latency_ms=100.0)
    assert cb.state == CircuitState.OPEN

    # Advance past cooldown to allow probe
    clock.advance(seconds=settings.circuit_breaker_cooldown_seconds + 1)
    assert cb.allow_call() is True
    assert cb.state == CircuitState.HALF_OPEN

    # Probe fails -> back to OPEN
    cb.record(success=False, latency_ms=100.0)
    assert cb.state == CircuitState.OPEN
    assert cb.allow_call() is False
