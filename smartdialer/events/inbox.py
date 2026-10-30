"""
events/inbox.py — Event ingestion with at-most-once storage per (provider, event_id).

Guarantee: exactly one row in call_events per (provider, event_id).

How deduplication works
-----------------------
INSERT INTO call_events (...) ON CONFLICT (provider, event_id) DO NOTHING

The UNIQUE INDEX uq_call_events_provider_event (in schema.sql) enforces the
uniqueness at the DB level.  If two threads race to ingest the same event_id,
one INSERT wins and the other is silently discarded.  No application-level
locking is needed.

Commit before processing
------------------------
The ingest() function commits the row immediately after insertion.  This
ensures the event is durable before any processing begins.  If the process
crashes between ingest and processing, the row survives and the processor
will replay it on the next tick.  This satisfies principle 8 (don't swallow)
and the "crash after ingest, before processing" test requirement.

call_id resolution
------------------
When ingested, the event's call_id may not be known yet (the provider callback
may race the executor's Txn 2).  We look up calls.provider_call_id to resolve.
If not found, call_id is left NULL; the processor will retry via QUARANTINED.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from smartdialer.clock import Clock, SystemClock
from smartdialer.db.models import Call, CallEvent
from smartdialer.providers.base import ProviderEvent

logger = logging.getLogger(__name__)


def ingest(
    session: Session,
    provider_name: str,
    event: ProviderEvent,
    clock: Clock | None = None,
) -> bool:
    """
    Store a raw provider event in call_events.

    Commits the row immediately so it survives crashes before processing.

    Parameters
    ----------
    session       : An open Session.  This function commits internally.
    provider_name : Name of the provider (e.g. "mock_a").
    event         : Raw ProviderEvent from poll_events().
    clock         : Injectable clock for received_at timestamp.

    Returns
    -------
    True  if the row was newly inserted (first time this event_id was seen).
    False if the row already existed (duplicate — DO NOTHING applied).
    """
    _clock = clock or SystemClock()
    now = _clock.now()

    # Try to resolve call_id from provider_call_id.
    call_id = _resolve_call_id(session, provider_name, event.provider_call_id)

    payload = {
        "provider_call_id": event.provider_call_id,
        "event_type": event.event_type.value,
        "seq": event.seq,
        "provider_ts": event.provider_ts.isoformat() if event.provider_ts else None,
    }

    # INSERT ... ON CONFLICT (provider, event_id) DO NOTHING
    # We use raw SQL here because SQLAlchemy's ORM insert doesn't natively
    # expose ON CONFLICT DO NOTHING with a composite index in a clean way.
    result = session.execute(
        text("""
            INSERT INTO call_events
              (provider, event_id, call_id, provider_call_id, event_type,
               provider_ts, received_at, payload, status, reason, attempts,
               next_retry_at, seq)
            VALUES
              (:provider, :event_id, :call_id, :provider_call_id, :event_type,
               :provider_ts, :received_at, CAST(:payload AS jsonb), 'RECEIVED',
               NULL, 0, NULL, :seq)
            ON CONFLICT (provider, event_id) DO NOTHING
        """),
        {
            "provider": provider_name,
            "event_id": event.event_id,
            "call_id": str(call_id) if call_id else None,
            "provider_call_id": event.provider_call_id,
            "event_type": event.event_type.value,
            "provider_ts": event.provider_ts,
            "received_at": now,
            "payload": json.dumps(payload),
            "seq": event.seq,
        },
    )
    inserted = result.rowcount == 1
    session.commit()

    if inserted:
        logger.debug(
            json.dumps({
                "event": "inbox.ingested",
                "provider": provider_name,
                "event_id": event.event_id,
                "provider_call_id": event.provider_call_id,
                "call_id": str(call_id) if call_id else None,
            })
        )
    else:
        logger.debug(
            json.dumps({
                "event": "inbox.duplicate_dropped",
                "provider": provider_name,
                "event_id": event.event_id,
            })
        )

    return inserted


def _resolve_call_id(
    session: Session, provider_name: str, provider_call_id: str
) -> "uuid.UUID | None":
    """
    Look up calls.id from calls.provider_call_id + provider.

    Returns None if no matching call row exists yet.  The processor will
    retry the event later via QUARANTINED status.
    """
    import uuid
    row = session.execute(
        select(Call.id).where(
            Call.provider == provider_name,
            Call.provider_call_id == provider_call_id,
        )
    ).scalar_one_or_none()
    return row
