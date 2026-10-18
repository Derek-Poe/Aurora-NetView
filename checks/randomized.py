"""Independent Python-count oracle and source immutability check."""

import collections
import hashlib
import json
import random
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from aurora_netview.core import dashboard, poll, refresh, rollups

randomizer = random.Random(447820)
checks = 0
with tempfile.TemporaryDirectory(prefix="aurora-independent-") as temp:
    for scenario in range(8):
        source = Path(temp) / f"source-{scenario}.sqlite"
        cache = Path(temp) / f"cache-{scenario}.sqlite"
        db = sqlite3.connect(source)
        db.executescript("""
            CREATE TABLE query_storage(id INTEGER PRIMARY KEY, timestamp INTEGER, domain INTEGER, client INTEGER);
            CREATE TABLE domain_by_id(id INTEGER PRIMARY KEY, domain TEXT);
            CREATE TABLE client_by_id(id INTEGER PRIMARY KEY, ip TEXT);
        """)
        clients = {i: f"192.0.2.{i}" for i in range(1, 8)}
        domains = {i: f"case-{i}.example" for i in range(1, 10)}
        db.executemany("INSERT INTO client_by_id VALUES (?,?)", clients.items())
        db.executemany("INSERT INTO domain_by_id VALUES (?,?)", domains.items())
        db.commit()
        history = []
        identity = f"independent-synthetic-{scenario}"
        query_id = 0
        now = 1800000000
        retention = 2 + scenario
        for cycle in range(12):
            now += randomizer.randrange(0, 2000)
            new = []
            for _ in range(randomizer.randrange(15, 80)):
                query_id += randomizer.randrange(1, 5)
                event = (
                    query_id,
                    now - randomizer.randrange(0, 50000),
                    randomizer.randrange(1, 10),
                    randomizer.randrange(1, 8),
                )
                history.append(event)
                new.append(event)
            db.executemany("INSERT INTO query_storage VALUES (?,?,?,?)", new)
            db.commit()
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            while poll(
                source,
                cache,
                source_id=identity,
                now=now,
                batch_size=randomizer.randrange(1, 31),
                retention_hours=retention,
            )["remaining"]:
                pass
            after = hashlib.sha256(source.read_bytes()).hexdigest()
            assert before == after, "Reader modified source database"
            cutoff = now // 3600 * 3600 - retention * 3600
            expected = {
                name: collections.Counter()
                for name in [
                    "hourly_totals",
                    "hourly_domains",
                    "hourly_clients",
                    "hourly_client_domains",
                ]
            }
            for _, timestamp, domain_id, client_id in history:
                if cutoff <= timestamp <= now:
                    hour = timestamp // 3600 * 3600
                    client, domain = clients[client_id], domains[domain_id]
                    expected["hourly_totals"][(hour,)] += 1
                    expected["hourly_domains"][(hour, domain)] += 1
                    expected["hourly_clients"][(hour, client)] += 1
                    expected["hourly_client_domains"][(hour, client, domain)] += 1
            actual = rollups(cache)
            assert actual == expected, (scenario, cycle)
            poll(source, cache, source_id=identity, now=now, retention_hours=retention)
            assert rollups(cache) == actual, "Replay changed counts"
            for _ in range(8):
                refresh(cache, now=now, limit=1)
            report = dashboard(cache, now=now)
            assert report["caught_up_at_poll"] and not any(
                c["stale"] for c in report["clients"]
            )
            assert report["queries"] == sum(expected["hourly_totals"].values())
            checks += 1
        db.close()
record = {
    "passed": True,
    "scenarios": 8,
    "checkpoints": checks,
    "seed": 447820,
    "oracle": "independent Python counters from generated event history",
    "verified": [
        "all four rollups",
        "late events and duplicate timestamps",
        "gapped increasing IDs",
        "retention advance",
        "replay idempotence",
        "bounded fair refresh",
        "source bytes unchanged",
    ],
}
print(json.dumps(record))
