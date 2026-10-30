"""
Injectable clock abstraction.

Why this exists
---------------
Any code that calls datetime.now() directly is untestable in a deterministic
way — you cannot control what "now" is.  By injecting a Clock, tests use
FakeClock (fully deterministic, no wall-clock dependency) while production
uses SystemClock.

Usage
-----
    def reserve(session, campaign_id, clock: Clock):
        now = clock.now()
        expires = now + timedelta(seconds=settings.agent_reservation_ttl_seconds)
        ...

    # In tests:
    clock = FakeClock(datetime(2024, 1, 1, tzinfo=timezone.utc))
    clock.advance(seconds=10)

    # In production:
    clock = SystemClock()
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    """Minimal clock interface.  Every component receives one of these."""

    def now(self) -> datetime:
        """Return the current UTC time as a timezone-aware datetime."""
        ...


class SystemClock:
    """Reads the real wall clock.  Used in all production code paths."""

    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


class FakeClock:
    """
    Deterministic clock for tests and simulations.

    The clock starts at a fixed point and only advances when you explicitly
    call advance().  This makes tests reproducible regardless of machine speed.
    """

    def __init__(self, start: datetime | None = None) -> None:
        if start is None:
            # Default epoch: easy to reason about in test assertions.
            start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        if start.tzinfo is None:
            raise ValueError("FakeClock start time must be timezone-aware.")
        self._current: datetime = start

    def now(self) -> datetime:
        return self._current

    def advance(self, seconds: float = 0, **kwargs) -> None:
        """
        Move the clock forward by the given duration.
        Accepts the same keyword arguments as timedelta (seconds, minutes, etc.).
        """
        self._current += timedelta(seconds=seconds, **kwargs)

    def set(self, dt: datetime) -> None:
        """Jump the clock to an absolute point in time."""
        if dt.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware.")
        self._current = dt
