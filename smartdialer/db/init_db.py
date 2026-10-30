"""
db/init_db.py — Schema initialisation script.

Run as:  python -m smartdialer.db.init_db [--test]

Steps
-----
1. Drop all tables (if --drop flag provided; destructive, dev only).
2. SQLAlchemy Base.metadata.create_all() — creates tables and inline
   CHECK constraints declared in models.py.
3. Apply schema.sql idempotently — partial indexes and any DDL that
   SQLAlchemy cannot express as create_all()-compatible declarative syntax.

We do NOT use Alembic.  The schema is always recreated from source; for
production, you would gate the drop behind an explicit flag and run
migrations manually.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

from sqlalchemy import text

from smartdialer.config import settings
from smartdialer.db.models import Base
from smartdialer.db.session import make_engine

SCHEMA_SQL = pathlib.Path(__file__).parent / "schema.sql"


def init_db(url: str | None = None, drop: bool = False) -> None:
    db_url = url or settings.db_url
    engine = make_engine(db_url)
    with engine.begin() as conn:
        if drop:
            print(f"  [init_db] Dropping all tables on {db_url} …")
            Base.metadata.drop_all(bind=conn)

        print(f"  [init_db] Running create_all on {db_url} …")
        Base.metadata.create_all(bind=conn)

        print(f"  [init_db] Applying schema.sql on {db_url} …")
        sql = SCHEMA_SQL.read_text(encoding="utf-8")
        # Execute each statement separately so psycopg doesn't reject the batch.
        for statement in _split_statements(sql):
            conn.execute(text(statement))

    print(f"  [init_db] Done.")


def _split_statements(sql: str) -> list[str]:
    """Split a SQL script on semicolons, skipping empty/comment-only chunks."""
    parts = []
    for chunk in sql.split(";"):
        stripped = chunk.strip()
        if stripped and not all(
            line.startswith("--") for line in stripped.splitlines() if line.strip()
        ):
            parts.append(stripped)
    return parts


if __name__ == "__main__":
    from smartdialer.config import settings

    parser = argparse.ArgumentParser(description="Initialise SmartDialer schema.")
    parser.add_argument(
        "--test", action="store_true", help="Target smartdialer_test database."
    )
    parser.add_argument(
        "--drop", action="store_true", help="Drop all tables first (destructive!)."
    )
    args = parser.parse_args()

    url = settings.test_db_url if args.test else settings.db_url
    print(f"[init_db] Targeting: {url}")
    init_db(url, drop=args.drop)
