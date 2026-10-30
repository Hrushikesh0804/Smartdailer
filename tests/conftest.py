"""
tests/conftest.py — Shared pytest fixtures for all tests.

Every test gets a clean slate: tables are TRUNCATED (not dropped/recreated)
between tests because TRUNCATE is much faster than DROP+CREATE.
TRUNCATE ... CASCADE handles foreign-key order automatically.

The engine points at smartdialer_test (never the production DB).
Each test gets its own Session; it is closed (and the transaction rolled back
if open) in the fixture teardown.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy.orm import Session

from smartdialer.config import settings
from smartdialer.db.init_db import init_db
from smartdialer.db.models import Base
from smartdialer.db.session import make_engine, make_session_factory

# ------------------------------------------------------------------ #
# Engine / schema setup (session-scoped: run once per pytest session)  #
# ------------------------------------------------------------------ #

TEST_DB_URL = os.environ.get("TEST_DB_URL", settings.test_db_url)


@pytest.fixture(scope="session")
def db_engine():
    """Create the test DB engine and initialise the schema once per test run."""
    engine = make_engine(TEST_DB_URL)
    # Drop + recreate so each test run starts with a known schema.
    init_db(TEST_DB_URL, drop=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def session_factory(db_engine):
    from smartdialer.db.session import set_default_session_factory
    factory = make_session_factory(db_engine)
    set_default_session_factory(factory)
    return factory


# ------------------------------------------------------------------ #
# Per-test truncation                                                  #
# ------------------------------------------------------------------ #

@pytest.fixture(autouse=True)
def truncate_tables(db_engine):
    """
    Truncate all tables before each test.

    We truncate rather than drop so the schema (indexes, constraints) stays
    in place.  RESTART IDENTITY resets sequences so IDs start from 1 again,
    making test assertions predictable.
    """
    with db_engine.begin() as conn:
        # Truncate in dependency order (children first) to satisfy FK constraints.
        conn.execute(
            __import__("sqlalchemy").text(
                "TRUNCATE operator_actions, metrics_snapshots, pacing_decisions, safety_decisions, system_flags, call_events, calls, borrowers, agents RESTART IDENTITY CASCADE"
            )
        )
    from smartdialer.providers.registry import clear, _register_defaults
    clear()
    _register_defaults()
    yield
    clear()
    _register_defaults()



# ------------------------------------------------------------------ #
# Per-test session                                                     #
# ------------------------------------------------------------------ #

@pytest.fixture
def session(session_factory) -> Session:
    """
    Yield a fresh Session for one test.

    The session is closed (rolling back any uncommitted state) in teardown.
    Tests that want to commit should call session.commit() explicitly or use
    the `transaction` context manager.
    """
    s = session_factory()
    try:
        yield s
    finally:
        s.close()
