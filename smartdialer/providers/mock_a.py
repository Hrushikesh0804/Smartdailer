"""
providers/mock_a.py — Mock Provider A (reliable, ordered).

Characteristics
---------------
* Low latency: simulated 50–150 ms round-trip.
* ~1 % rejection rate (configurable via rejection_rate).
* Events delivered in strict sequence order (RINGING -> ANSWERED -> COMPLETED/FAILED).
* No duplicate events.
* Supports both idempotency keys and status queries.
* Re-sending initiate_call with the same idempotency_key returns the original
  result without creating a second call.
* Outage mode: set_outage(True) makes all initiations return TIMEOUT and
  poll_events() return [].

Randomness
----------
seeded random.Random(seed) injected at construction — no global state,
fully deterministic in tests.

Clock injection
---------------
latency_ms is computed by calling clock.now() twice (before and after the
simulated processing).  No real sleeps.  The clock is advanced internally
by the simulated latency so the timestamps in ProviderEvent are consistent.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Optional

from smartdialer.clock import Clock, SystemClock
from smartdialer.providers.base import (
    InitiateResult,
    InitiateStatus,
    ProviderEvent,
    ProviderEventType,
    StatusResult,
)

import random as _random_module


class MockProviderA:
    """Mock Provider A: reliable, ordered events, supports idempotency + status."""

    name: str = "mock_a"
    supports_status_query: bool = True
    supports_idempotency_key: bool = True

    def __init__(
        self,
        seed: int = 1,
        rejection_rate: float = 0.01,
        min_latency_ms: float = 50.0,
        max_latency_ms: float = 150.0,
        clock: Clock | None = None,
        outcome_fn: Any | None = None,
    ) -> None:
        self._rng = _random_module.Random(seed)
        self._rejection_rate = rejection_rate
        self._min_latency_ms = min_latency_ms
        self._max_latency_ms = max_latency_ms
        self._clock = clock or SystemClock()
        self._outage: bool = False
        self.outcome_fn = outcome_fn

        # call_id (idempotency key) -> provider_call_id
        self._idempotency_map: dict[str, str] = {}
        # provider_call_id -> list of pending ProviderEvent (ordered queue)
        self._pending_events: list[ProviderEvent] = []
        # provider_call_id -> current simulated status
        self._call_status: dict[str, ProviderEventType] = {}
        self._seq_counter: int = 0

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def set_outage(self, active: bool) -> None:
        self._outage = active

    def health_snapshot(self) -> dict[str, Any]:
        if getattr(self, "_outage", False):
            return {
                "circuit_state": "OPEN",
                "failure_rate": 1.0,
                "latency_p50_ms": 999.0,
            }
        return {
            "circuit_state": "CLOSED",
            "failure_rate": self._rejection_rate,
            "latency_p50_ms": (self._min_latency_ms + self._max_latency_ms) / 2.0,
        }

    def initiate_call(
        self,
        call_id: uuid.UUID,
        idempotency_key: str,
        to_token: str,
    ) -> InitiateResult:
        t0 = self._clock.now()
        latency_ms = self._rng.uniform(self._min_latency_ms, self._max_latency_ms)

        # Idempotency: re-send with same key returns original result.
        if idempotency_key in self._idempotency_map:
            provider_call_id = self._idempotency_map[idempotency_key]
            return InitiateResult(
                status=InitiateStatus.ACCEPTED,
                provider_call_id=provider_call_id,
                latency_ms=latency_ms,
            )

        if self._outage:
            return InitiateResult(
                status=InitiateStatus.TIMEOUT,
                latency_ms=latency_ms,
                error_message="Provider A simulated outage",
            )

        if self._rng.random() < self._rejection_rate:
            return InitiateResult(
                status=InitiateStatus.REJECTED,
                latency_ms=latency_ms,
                error_message="Provider A rejected: number not reachable",
            )

        # Create the call.
        provider_call_id = f"pca_{uuid.uuid4().hex[:12]}"
        self._idempotency_map[idempotency_key] = provider_call_id
        self._call_status[provider_call_id] = ProviderEventType.RINGING

        # Queue ordered events based on outcome_fn or default
        if self.outcome_fn:
            outcome = self.outcome_fn(call_id, t0)
            self._enqueue_event(provider_call_id, ProviderEventType.RINGING, delay_ms=latency_ms + 100)
            ring_ms = max(500.0, outcome.ring_time_seconds * 1000.0)
            if outcome.answered:
                self._enqueue_event(provider_call_id, ProviderEventType.ANSWERED, delay_ms=latency_ms + ring_ms)
                talk_ms = max(500.0, outcome.talk_time_seconds * 1000.0)
                self._enqueue_event(provider_call_id, ProviderEventType.COMPLETED, delay_ms=latency_ms + ring_ms + talk_ms)
            else:
                self._enqueue_event(provider_call_id, ProviderEventType.FAILED, delay_ms=latency_ms + ring_ms)
        else:
            self._enqueue_event(provider_call_id, ProviderEventType.RINGING, delay_ms=latency_ms + 100)
            self._enqueue_event(provider_call_id, ProviderEventType.ANSWERED, delay_ms=latency_ms + 500)
            self._enqueue_event(provider_call_id, ProviderEventType.COMPLETED, delay_ms=latency_ms + 3000)

        return InitiateResult(
            status=InitiateStatus.ACCEPTED,
            provider_call_id=provider_call_id,
            latency_ms=latency_ms,
        )

    def get_call_status(self, provider_call_id: str) -> StatusResult | None:
        """Return current simulated status, or None if call not found."""
        if self._outage:
            return None
        pid = self._idempotency_map.get(provider_call_id, provider_call_id)
        event_type = self._call_status.get(pid)
        if event_type is None:
            return None
        return StatusResult(
            provider_call_id=pid,
            event_type=event_type,
            provider_ts=self._clock.now(),
        )

    def poll_events(self, max_n: int = 100, ready_only: bool = False) -> list[ProviderEvent]:
        """Return up to max_n pending events. If ready_only, returns only events with provider_ts <= now."""
        if self._outage:
            return []
        if ready_only:
            now = self._clock.now()
            ready_indices = [i for i, e in enumerate(self._pending_events) if e.provider_ts <= now]
            selected_indices = ready_indices[:max_n]
            batch = [self._pending_events[i] for i in selected_indices]
            for i in reversed(selected_indices):
                self._pending_events.pop(i)
        else:
            batch = self._pending_events[:max_n]
            self._pending_events = self._pending_events[max_n:]

        # Update status to last event type seen.
        for evt in batch:
            self._call_status[evt.provider_call_id] = evt.event_type
        return batch


    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _enqueue_event(
        self,
        provider_call_id: str,
        event_type: ProviderEventType,
        delay_ms: float,
    ) -> None:
        self._seq_counter += 1
        event_id = f"evA_{provider_call_id}_{event_type.value}_{self._seq_counter}"
        evt = ProviderEvent(
            event_id=event_id,
            provider_call_id=provider_call_id,
            event_type=event_type,
            provider_ts=self._clock.now() + timedelta(milliseconds=delay_ms),
            seq=self._seq_counter,
        )
        self._pending_events.append(evt)
