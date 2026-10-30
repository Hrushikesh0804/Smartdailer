"""
providers/base.py — Provider Protocol and result DTOs.

Design contract
---------------
* TelecomProvider is a typing.Protocol, not an ABC.  Any class with matching
  method signatures satisfies the protocol — no inheritance required.
* Result types are pydantic BaseModel so they can be safely serialised to
  JSON for logging, auditing, and test assertions.
* The provider interface is intentionally minimal.  Provider-specific quirks
  (duplicate events, out-of-order delivery, idempotency oddities) are handled
  INSIDE each provider implementation, never leaked to callers.
* `set_outage(bool)` is the only testing seam — callers use this to inject
  failures; they never monkey-patch internal state.
* latency_ms is the *simulated* latency (wall-clock delta between Clock.now()
  calls inside the provider), not a real sleep.

Why Protocol over ABC?
  ABCs require inheritance, which creates tight coupling and makes it harder
  to wrap third-party clients.  A Protocol means any object that has the right
  methods works — including vendor SDKs wrapped with a thin adapter.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from pydantic import BaseModel


# --------------------------------------------------------------------------- #
# Enums                                                                        #
# --------------------------------------------------------------------------- #

class InitiateStatus(str, Enum):
    ACCEPTED = "ACCEPTED"   # Provider confirmed call was created.
    REJECTED = "REJECTED"   # Provider declined (e.g. invalid number, campaign blocked).
    TIMEOUT  = "TIMEOUT"    # We did not get a response in time.  State is AMBIGUOUS.


class ProviderEventType(str, Enum):
    RINGING   = "RINGING"
    ANSWERED  = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"


# --------------------------------------------------------------------------- #
# Result DTOs                                                                  #
# --------------------------------------------------------------------------- #

class InitiateResult(BaseModel):
    """
    Result of a provider.initiate_call() call.

    `provider_call_id` is only meaningful when status=ACCEPTED.  On TIMEOUT
    it may be None (we don't know if the call was created).  On REJECTED it
    is None.

    `latency_ms` is the simulated round-trip time.  In the mock providers this
    is computed from injected Clock timestamps so tests are deterministic.
    """
    status: InitiateStatus
    provider_call_id: str | None = None
    latency_ms: float = 0.0
    error_message: str | None = None


class StatusResult(BaseModel):
    """
    Result of a provider.get_call_status() query.

    Used by the sweeper to reconcile UNKNOWN-state calls on providers that
    support status queries (MockProviderA).
    """
    provider_call_id: str
    event_type: ProviderEventType
    provider_ts: datetime | None = None


class ProviderEvent(BaseModel):
    """
    One raw event returned by provider.poll_events().

    `event_id` uniquely identifies this event within the provider.  Combined
    with the provider name it forms the UNIQUE dedup key in call_events.

    `seq` is an optional sequence number some providers include to help
    detect gaps or ordering issues.
    """
    event_id: str
    provider_call_id: str
    event_type: ProviderEventType
    provider_ts: datetime | None = None
    seq: int | None = None


# --------------------------------------------------------------------------- #
# Protocol                                                                     #
# --------------------------------------------------------------------------- #

@runtime_checkable
class TelecomProvider(Protocol):
    """
    Minimal interface every provider adapter must satisfy.

    Methods
    -------
    initiate_call(call_id, idempotency_key, to_token)
        Ask the provider to place an outbound call.  Must NEVER be called
        inside a DB transaction (principle 3).

    get_call_status(provider_call_id)
        Query the current status of a specific call leg.  Returns None if
        the call is not found, raises NotImplementedError if unsupported.

    poll_events(max_n)
        Retrieve up to max_n pending events from the provider's event queue.
        Events may be out-of-order or duplicated; the inbox handles dedup.

    set_outage(active)
        Testing seam: when True, all calls to initiate_call return TIMEOUT
        and poll_events returns [].

    Properties
    ----------
    name              : str  — provider identifier, matches calls.provider column.
    supports_status_query   : bool — whether get_call_status() is implemented.
    supports_idempotency_key: bool — whether re-sending with the same key is safe.
    """

    name: str
    supports_status_query: bool
    supports_idempotency_key: bool

    def initiate_call(
        self,
        call_id: uuid.UUID,
        idempotency_key: str,
        to_token: str,
    ) -> InitiateResult: ...

    def get_call_status(self, provider_call_id: str) -> StatusResult | None: ...

    def poll_events(self, max_n: int = 100) -> list[ProviderEvent]: ...

    def set_outage(self, active: bool) -> None: ...
