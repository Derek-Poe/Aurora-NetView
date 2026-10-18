"""Read-only source ingestion; four transactional hourly materialized views."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

TABLES = {
    "hourly_totals": ("hour",),
    "hourly_domains": ("hour", "domain"),
    "hourly_clients": ("hour", "client"),
    "hourly_client_domains": ("hour", "client", "domain"),
}


class SourceChanged(ValueError):
    """The checkpoint cannot safely be continued; use a new cache/rebuild."""


def readonly(path):
    conn = sqlite3.connect(
        Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=10
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


class PiHoleSource:
    """Normalized integer-reference schema only. Never writes to the source."""

    query = """SELECT q.id, q.timestamp, c.ip AS client, d.domain,
                      typeof(q.domain) AS dt, typeof(q.client) AS ct
               FROM query_storage q
               LEFT JOIN domain_by_id d ON q.domain=d.id
               LEFT JOIN client_by_id c ON q.client=c.id """

    def __init__(self, conn):
        self.conn = conn
        for table, columns in {
            "query_storage": {"id", "timestamp", "domain", "client"},
            "domain_by_id": {"id", "domain"},
            "client_by_id": {"id", "ip"},
        }.items():
            info = conn.execute(f"PRAGMA table_info({table})").fetchall()
            if not columns <= {r["name"] for r in info}:
                raise ValueError("Unsupported normalized Pi-hole schema: " + table)
            if not any(
                r["name"] == "id" and r["pk"] == 1 and r["type"].upper() == "INTEGER"
                for r in info
            ):
                raise ValueError("Source tables require INTEGER primary-key IDs")

    def rows(self, after=0, limit=1000):
        for row in self.conn.execute(
            self.query + " WHERE q.id > ? ORDER BY q.id LIMIT ?", (after, limit)
        ):
            if row["dt"] != "integer" or row["ct"] != "integer":
                raise ValueError(
                    "Legacy string-valued source references are unsupported"
                )
            if (
                type(row["id"]) is not int
                or row["id"] <= 0
                or type(row["timestamp"]) is not int
                or row["timestamp"] < 0
            ):
                raise ValueError(
                    "Source IDs and timestamps must be nonnegative integers (IDs positive)"
                )
            if (
                not isinstance(row["client"], str)
                or not row["client"]
                or not isinstance(row["domain"], str)
                or not row["domain"]
            ):
                raise ValueError("Missing or invalid source dimension; batch rejected")
            yield (row["id"], row["timestamp"], row["client"], row["domain"])

    def maximum(self):
        minimum = self.conn.execute("SELECT MIN(id) FROM query_storage").fetchone()[0]
        if minimum is not None and minimum <= 0:
            raise ValueError("Source IDs must be positive")
        return self.conn.execute(
            "SELECT COALESCE(MAX(id),0) FROM query_storage"
        ).fetchone()[0]

    def checkpoint(self, cursor):
        rows = list(self.rows(cursor - 1, 1))
        return fingerprint(rows[0]) if rows and rows[0][0] == cursor else None


def fingerprint(row):
    return hashlib.sha256(json.dumps(row, separators=(",", ":")).encode()).hexdigest()


def separate(source, cache):
    a, b = Path(source), Path(cache)
    if a.resolve() == b.resolve() or (a.exists() and b.exists() and a.samefile(b)):
        raise ValueError("Source and cache must be distinct files")


def open_cache(path):
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript("""
      CREATE TABLE IF NOT EXISTS state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), source_id TEXT NOT NULL,
        cursor INTEGER NOT NULL, tail TEXT, last_poll INTEGER NOT NULL,
        retention_hours INTEGER NOT NULL, processed INTEGER NOT NULL, expired INTEGER NOT NULL,
        last_refresh INTEGER, observed_max INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS snapshots (
        client TEXT PRIMARY KEY, total INTEGER NOT NULL, dirty INTEGER NOT NULL,
        refreshed INTEGER, content TEXT);
    """)
    for table, keys in TABLES.items():
        cols = ", ".join(
            k + (" INTEGER" if k == "hour" else " TEXT") + " NOT NULL" for k in keys
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ({cols}, hits INTEGER NOT NULL, PRIMARY KEY ({','.join(keys)}))"
        )
    conn.commit()
    return conn


def keys_for(row):
    _, ts, client, domain = row
    h = ts // 3600 * 3600
    return ((h,), (h, domain), (h, client), (h, client, domain))


def poll(
    source,
    cache,
    *,
    source_id,
    now,
    batch_size=1000,
    retention_hours=24,
    _before_commit=None,
):
    """Consume at most batch_size rows. Cursor, counts, expiry and poll health commit together."""
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("A configured source identity is required")
    if (
        type(now) is not int
        or now < 0
        or type(batch_size) is not int
        or not 1 <= batch_size <= 100_000
    ):
        raise ValueError("Invalid clock or batch size")
    if type(retention_hours) is not int or not 1 <= retention_hours <= 8760:
        raise ValueError("Retention must be 1..8760 hours")
    separate(source, cache)
    cutoff = now // 3600 * 3600 - retention_hours * 3600
    with closing(open_cache(cache)) as dst, dst:
        dst.execute("BEGIN IMMEDIATE")  # serialize cursor readers as well as writers
        old = dst.execute("SELECT * FROM state WHERE singleton=1").fetchone()
        cursor, tail, processed, expired = (
            (old["cursor"], old["tail"], old["processed"], old["expired"])
            if old
            else (0, None, 0, 0)
        )
        if old and (
            old["source_id"] != source_id or old["retention_hours"] != retention_hours
        ):
            raise SourceChanged("Source identity or retention changed; use a new cache")
        if old and now < max(old["last_poll"], old["last_refresh"] or 0):
            raise ValueError("Clock moved backwards")
        with closing(readonly(source)) as src:
            src.execute("BEGIN")
            adapter = PiHoleSource(src)
            maximum = adapter.maximum()
            if maximum < cursor or (cursor and adapter.checkpoint(cursor) != tail):
                raise SourceChanged(
                    "Source reset, checkpoint mutation or checkpoint pruning detected"
                )
            rows = list(adapter.rows(cursor, batch_size))
            touched = {
                r[0]
                for r in dst.execute(
                    "SELECT DISTINCT client FROM hourly_clients WHERE hour < ?",
                    (cutoff,),
                )
            }
            for row in rows:
                if row[1] > now:
                    raise ValueError("Future timestamp: entire batch rejected")
                cursor, tail = row[0], fingerprint(row)
                processed += 1
                if row[1] < cutoff:
                    expired += 1
                    continue
                touched.add(row[2])
                for (table, keys), values in zip(TABLES.items(), keys_for(row)):
                    names = ",".join(keys)
                    dst.execute(
                        f"INSERT INTO {table} ({names},hits) VALUES ({','.join('?' for _ in keys)},1) ON CONFLICT({names}) DO UPDATE SET hits=hits+1",
                        values,
                    )
            for table in TABLES:
                dst.execute(f"DELETE FROM {table} WHERE hour < ?", (cutoff,))
            dst.execute(
                "DELETE FROM snapshots WHERE client NOT IN (SELECT client FROM hourly_clients)"
            )
            for client in sorted(touched):
                total = dst.execute(
                    "SELECT COALESCE(SUM(hits),0) FROM hourly_clients WHERE client=?",
                    (client,),
                ).fetchone()[0]
                if total:
                    dst.execute(
                        "INSERT INTO snapshots(client,total,dirty) VALUES(?,?,1) ON CONFLICT(client) DO UPDATE SET total=excluded.total,dirty=1",
                        (client, total),
                    )
            dst.execute(
                "INSERT INTO state(singleton,source_id,cursor,tail,last_poll,retention_hours,processed,expired,observed_max) VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET cursor=excluded.cursor,tail=excluded.tail,last_poll=excluded.last_poll,processed=excluded.processed,expired=excluded.expired,observed_max=excluded.observed_max",
                (
                    source_id,
                    cursor,
                    tail,
                    now,
                    retention_hours,
                    processed,
                    expired,
                    maximum,
                ),
            )
            if _before_commit:
                _before_commit()
        return {
            "read": len(rows),
            "cursor": cursor,
            "remaining": maximum > cursor,
            "processed": processed,
            "expired_on_arrival": expired,
            "as_of": now,
        }


def refresh(cache, *, now, limit=20, ttl=1800):
    """Fair deterministic cached client summaries: structured counts, no inference."""
    if (
        type(now) is not int
        or now < 0
        or type(limit) is not int
        or not 1 <= limit <= 10_000
        or type(ttl) is not int
        or ttl < 1
    ):
        raise ValueError("Invalid refresh parameters")
    with closing(open_cache(cache)) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        state = conn.execute("SELECT last_poll,last_refresh FROM state").fetchone()
        if state is None or now < max(state[0], state[1] or 0):
            raise ValueError(
                "Refresh requires a successful poll and a nondecreasing clock"
            )
        due = conn.execute(
            "SELECT client FROM snapshots WHERE dirty=1 OR refreshed IS NULL OR refreshed<=? ORDER BY COALESCE(refreshed,-1),client LIMIT ?",
            (now - ttl, limit),
        ).fetchall()
        for item in due:
            client = item[0]
            domains = [
                dict(r)
                for r in conn.execute(
                    "SELECT domain,SUM(hits) AS hits FROM hourly_client_domains WHERE client=? GROUP BY domain ORDER BY hits DESC,domain LIMIT 10",
                    (client,),
                )
            ]
            conn.execute(
                "UPDATE snapshots SET refreshed=?,dirty=0,content=? WHERE client=?",
                (now, json.dumps(domains, separators=(",", ":")), client),
            )
        conn.execute("UPDATE state SET last_refresh=? WHERE singleton=1", (now,))
        return [r[0] for r in due]


def rollups(cache):
    with closing(readonly(cache)) as conn:
        conn.execute("BEGIN")
        return {
            table: {
                tuple(r[k] for k in keys): r["hits"]
                for r in conn.execute(f"SELECT * FROM {table}")
            }
            for table, keys in TABLES.items()
        }


def full_recompute(source, *, now, retention_hours=24):
    """Independent full-scan SQL GROUP BY oracle; no cache writes."""
    cutoff = now // 3600 * 3600 - retention_hours * 3600
    base = """ FROM query_storage q JOIN client_by_id c ON q.client=c.id
                JOIN domain_by_id d ON q.domain=d.id WHERE q.timestamp>=? AND q.timestamp<=? """
    expressions = {
        "hour": "(q.timestamp / 3600) * 3600",
        "client": "c.ip",
        "domain": "d.domain",
    }
    with closing(readonly(source)) as conn:
        conn.execute("BEGIN")
        PiHoleSource(conn)
        result = {}
        for table, keys in TABLES.items():
            columns = ",".join(expressions[k] for k in keys)
            result[table] = {
                tuple(row[:-1]): row[-1]
                for row in conn.execute(
                    "SELECT " + columns + ",COUNT(*)" + base + " GROUP BY " + columns,
                    (cutoff, now),
                )
            }
        return result


def dashboard(cache, *, now, ttl=1800):
    with closing(readonly(cache)) as conn:
        conn.execute("BEGIN")
        state = dict(conn.execute("SELECT * FROM state").fetchone())
        clients = []
        for r in conn.execute("SELECT * FROM snapshots ORDER BY total DESC,client"):
            clients.append(
                {
                    "client": r["client"],
                    "queries": r["total"],
                    "stale": bool(
                        r["dirty"]
                        or r["refreshed"] is None
                        or now - r["refreshed"] >= ttl
                    ),
                    "refreshed": r["refreshed"],
                    "domains": json.loads(r["content"] or "[]"),
                }
            )
        return {
            "as_of": state["last_poll"],
            "poll_age_seconds": max(0, now - state["last_poll"]),
            "cursor": state["cursor"],
            "observed_max_id": state["observed_max"],
            "caught_up_at_poll": state["cursor"] >= state["observed_max"],
            "processed": state["processed"],
            "expired_on_arrival": state["expired"],
            "retention_hours": state["retention_hours"],
            "queries": conn.execute(
                "SELECT COALESCE(SUM(hits),0) FROM hourly_totals"
            ).fetchone()[0],
            "clients": clients,
        }
