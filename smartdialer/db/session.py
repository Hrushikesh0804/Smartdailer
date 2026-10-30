"""
Database engine factory and session utilities.

Design decisions
----------------
* ONE Session per worker thread, NEVER shared across threads.
  SQLAlchemy's Session is not thread-safe; sharing it causes data corruption.
* Pool size is set from config so it can be tuned per-deployment without
  code changes.  The rule: pool_size >= worker_threads + 2 (the +2 covers
  init_db and health-check connections).
* Isolation level = READ COMMITTED (Postgres default).  Correctness comes
  from SELECT ... FOR UPDATE SKIP LOCKED and partial unique indexes, not from
  SERIALIZABLE (which would reduce throughput and cause spurious rollbacks).
* The `transaction` context manager is the only place we call commit/rollback.
  Business logic functions never commit; they only mutate ORM objects.
  This keeps the "caller owns the transaction" contract clean.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.config import settings


def make_engine(url: str | None = None):
    """
    Create a SQLAlchemy engine.

    Parameters
    ----------
    url:
        Override the DB URL (used by tests to point at smartdialer_test).
        Defaults to settings.db_url.
    """
    db_url = url or settings.db_url
    return create_engine(
        db_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_pool_max_overflow,
        # Echo SQL only when explicitly requested (set via env LOG_SQL=1 in future).
        echo=False,
        # Use READ COMMITTED for all sessions on this engine.
        isolation_level="READ COMMITTED",
    )


def make_session_factory(engine=None) -> sessionmaker[Session]:
    """Return a sessionmaker bound to the given engine."""
    if engine is None:
        engine = make_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


# Module-level defaults (production).  Tests create their own via make_engine().
_default_engine = None
_default_factory = None


def get_session_factory() -> sessionmaker[Session]:
    """Lazy-initialise and return the default session factory."""
    global _default_engine, _default_factory
    if _default_factory is None:
        _default_engine = make_engine()
        _default_factory = make_session_factory(_default_engine)
    return _default_factory


def set_default_session_factory(factory: sessionmaker[Session] | None) -> None:
    """Explicitly override the default session factory (useful for tests)."""
    global _default_factory
    _default_factory = factory


@contextmanager
def transaction(session: Session) -> Generator[Session, None, None]:
    """
    Context manager that commits on success and rolls back on any exception.

    Usage::

        with transaction(session) as s:
            s.add(some_object)
            do_work(s)
        # committed here

    The session is yielded so callers can write::

        with transaction(session):
            allocator.reserve(session, ...)

    Never call session.commit() or session.rollback() inside business logic.
    """
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
