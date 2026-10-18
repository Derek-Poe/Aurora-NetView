"""Generated Pi-hole-shaped fixtures. No real DNS data or device access."""

import sqlite3
from contextlib import closing
from pathlib import Path

DEMO_NOW = 1_800_000_000


def create(path):
    path = Path(path)
    # Reserve exclusively; never replace a user's existing database.
    with path.open("xb"):
        pass
    with closing(sqlite3.connect(path)) as conn:
        conn.executescript("""
          CREATE TABLE domain_by_id(id INTEGER PRIMARY KEY, domain TEXT NOT NULL UNIQUE);
          CREATE TABLE client_by_id(id INTEGER PRIMARY KEY, ip TEXT NOT NULL UNIQUE,name TEXT);
          CREATE TABLE query_storage(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp INTEGER NOT NULL,
                                     domain INTEGER NOT NULL,client INTEGER NOT NULL);
        """)


def append(path, events):
    """Fixture helper only; events are (epoch second, client address, domain)."""
    if not Path(path).is_file():
        raise ValueError("Create the synthetic fixture first")
    with closing(sqlite3.connect(path)) as conn, conn:
        for ts, client, domain in events:
            conn.execute(
                "INSERT OR IGNORE INTO domain_by_id(domain) VALUES(?)", (domain,)
            )
            conn.execute(
                "INSERT OR IGNORE INTO client_by_id(ip,name) VALUES(?,NULL)", (client,)
            )
            conn.execute(
                "INSERT INTO query_storage(timestamp,domain,client) SELECT ?,d.id,c.id FROM domain_by_id d,client_by_id c WHERE d.domain=? AND c.ip=?",
                (ts, domain, client),
            )


def events(count, *, now=DEMO_NOW, offset=0):
    for n in range(offset, offset + count):
        yield (
            now - 1 - (n * 37 % 72000),
            f"192.0.2.{1 + n % 24}",
            f"service-{n % 48}.example",
        )
