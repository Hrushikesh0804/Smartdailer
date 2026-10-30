"""
providers/mock_b.py — Mock Provider B (unreliable, out-of-order).

Characteristics
---------------
* Higher latency: simulated 200–800 ms round-trip.
* Configurable TIMEOUT rate (default 10 %).
* timeout_secretly_accepted_rate: fraction of TIMEOUTs where the provider
  actually DID create the call.  Models the ambiguous case: we timed out
  but the call may or may not exist.  Both outcomes are tested.
* Duplicate events: the same event_id may appear up to `max_duplicates` times.
* Out-of-order delivery: events are shuffled within a window.
* Supports idempotency keys (re-send with same key = same provider_call_id).
* Does NOT support status query — get_call_status() raises NotImplementedError.
* Outage mode: set_outage(True) -> all initiations return TIMEOUT.

Why model both timeout outcomes?
  Principle 6: "Timeouts are ambiguous.  Never blindly retry."
  We must test that:
  (a) when the provider did accept (timeout_secretly_accepted=True):
      the sweeper's idempotent re-send returns the existing call, not a new one.
  (b) when the provider did not accept (timeout_secretly_accepted=False):
      the sweeper's idempotent re-send creates a new call correctly.
  Both paths converge to a consistent DB state.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from smartdialer.clock import Clock, SystemClock
from smartdialer.providers.base import (
    InitiateResult,
    InitiateStatus,
    ProviderEvent,
    ProviderEventType,
    StatusResult,
)

import random as _random_module


class MockProviderB:
    """Mock Provider B: unreliable, out-of-order, duplicates, no status query."""

    name: str = "mock_b"
    supports_status_query: bool = False
    supports_idempotency_key: bool = True

    def __init__(
        self,
        seed: int = 2,
        timeout_rate: float = 0.10,
        timeout_secretly_accepted_rate: float = 0.5,
        rejection_rate: float = 0.05,
        min_latency_ms: float = 200.0,
        max_latency_ms: float = 800.0,
        max_duplicates: int = 3,
        shuffle_window: int = 4,
        clock: Clock | None = None,
        outcome_fn: Any | None = None,
    ) -> None:
        self._rng = _random_module.Random(seed)
        self._timeout_rate = timeout_rate
        self._timeout_secretly_accepted_rate = timeout_secretly_accepted_rate
        self._rejection_rate = rejection_rate
        self._min_latency_ms = min_latency_ms
        self._max_latency_ms = max_latency_ms
        self._max_duplicates = max_duplicates
        self._shuffle_window = shuffle_window
        self._clock = clock or SystemClock()
        self._outage: bool = False
        self.outcome_fn = outcome_fn


        # idempotency_key -> provider_call_id (or None if not accepted)
        self._idempotency_map: dict[str, str | None] = {}
        # event buffer (may contain duplicates, shuffled)
        self._event_buffer: list[ProviderEvent] = []
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
            "failure_rate": self._rejection_rate + self._timeout_rate,
            "latency_p50_ms": (self._min_latency_ms + self._max_latency_ms) / 2.0,
        }

    def initiate_call(
        self,
        call_id: uuid.UUID,
        idempotency_key: str,
        to_token: str,
    ) -> InitiateResult:
        latency_ms = self._rng.uniform(self._min_latency_ms, self._max_latency_ms)

        # Idempotency: re-send returns original result.
        if idempotency_key in self._idempotency_map:
            existing = self._idempotency_map[idempotency_key]
            if existing is None:
                # Original was not accepted; re-send creates a new call.
                return self._create_accepted(idempotency_key, latency_ms)
            return InitiateResult(
                status=InitiateStatus.ACCEPTED,
                provider_call_id=existing,
                latency_ms=latency_ms,
            )

        if self._outage:
            # Under outage, always timeout; mark as not-accepted.
            self._idempotency_map[idempotency_key] = None
            return InitiateResult(
                status=InitiateStatus.TIMEOUT,
                latency_ms=latency_ms,
                error_message="Provider B simulated outage",
            )

        # Normal rejection
        if self._rng.random() < self._rejection_rate:
            self._idempotency_map[idempotency_key] = None
            return InitiateResult(
                status=InitiateStatus.REJECTED,
                latency_ms=latency_ms,
                error_message="Provider B rejected: upstream error",
            )

        # Simulate timeout (ambiguous outcome).
        if self._rng.random() < self._timeout_rate:
            secretly_accepted = (
                self._rng.random() < self._timeout_secretly_accepted_rate
            )
            if secretly_accepted:
                # Provider DID accept, but we timed out before receiving the reply.
                # The idempotency map stores the real provider_call_id so a
                # subsequent re-send returns it.
                provider_call_id = f"pcb_{uuid.uuid4().hex[:12]}"
                self._idempotency_map[idempotency_key] = provider_call_id
                self._enqueue_events(provider_call_id, call_id=call_id)
            else:
                # Provider did NOT accept.  Re-send will create a new call.
                self._idempotency_map[idempotency_key] = None

            return InitiateResult(
                status=InitiateStatus.TIMEOUT,
                latency_ms=latency_ms,
                error_message="Provider B: request timed out",
            )

        return self._create_accepted(idempotency_key, latency_ms, call_id=call_id)

    def get_call_status(self, provider_call_id: str) -> StatusResult | None:
        raise NotImplementedError(
            "Provider B does not support status queries. "
            "Use idempotency key re-send for reconciliation."
        )

    def poll_events(self, max_n: int = 100, ready_only: bool = False) -> list[ProviderEvent]:
        if self._outage:
            return []
        if ready_only:
            now = self._clock.now()
            ready_indices = [i for i, e in enumerate(self._event_buffer) if e.provider_ts <= now]
            selected_indices = ready_indices[:max_n]
            batch = [self._event_buffer[i] for i in selected_indices]
            for i in reversed(selected_indices):
                self._event_buffer.pop(i)
        else:
            batch = self._event_buffer[:max_n]
            self._event_buffer = self._event_buffer[max_n:]
        return batch

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _create_accepted(self, idempotency_key: str, latency_ms: float, call_id: uuid.UUID | None = None) -> InitiateResult:
        provider_call_id = f"pcb_{uuid.uuid4().hex[:12]}"
        self._idempotency_map[idempotency_key] = provider_call_id
        self._enqueue_events(provider_call_id, call_id=call_id)
        return InitiateResult(
            status=InitiateStatus.ACCEPTED,
            provider_call_id=provider_call_id,
            latency_ms=latency_ms,
        )

    def _enqueue_events(self, provider_call_id: str, call_id: uuid.UUID | None = None) -> None:
        """
        Queue RINGING, ANSWERED, COMPLETED/FAILED events with duplicates and shuffling.
        """
        base_delay = self._rng.uniform(200, 600)
        ordered: list[ProviderEvent] = []

        if self.outcome_fn and call_id:
            outcome = self.outcome_fn(call_id, self._clock.now())
            ring_ms = max(500.0, outcome.ring_time_seconds * 1000.0)
            if outcome.answered:
                event_specs = [
                    (ProviderEventType.RINGING, 0),
                    (ProviderEventType.ANSWERED, ring_ms),
                    (ProviderEventType.COMPLETED, ring_ms + outcome.talk_time_seconds * 1000.0),
                ]
            else:
                event_specs = [
                    (ProviderEventType.RINGING, 0),
                    (ProviderEventType.FAILED, ring_ms),
                ]
        else:
            event_specs = [
                (ProviderEventType.RINGING, 0),
                (ProviderEventType.ANSWERED, 300),
                (ProviderEventType.COMPLETED, 2000),
            ]

        for event_type, delay_extra in event_specs:
            self._seq_counter += 1
            event_id = f"evB_{provider_call_id}_{event_type.value}"
            base_evt = ProviderEvent(
                event_id=event_id,
                provider_call_id=provider_call_id,
                event_type=event_type,
                provider_ts=self._clock.now() + timedelta(milliseconds=base_delay + delay_extra),
                seq=self._seq_counter,
            )
            # Add the canonical event plus duplicates (same event_id).
            n_copies = self._rng.randint(1, self._max_duplicates)
            ordered.extend([base_evt] * n_copies)

        # Shuffle in windows to simulate out-of-order delivery.
        shuffled: list[ProviderEvent] = []
        w = self._shuffle_window
        for i in range(0, len(ordered), w):
            window = ordered[i:i + w]
            self._rng.shuffle(window)
            shuffled.extend(window)

        self._event_buffer.extend(shuffled)

