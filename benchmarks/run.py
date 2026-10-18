"""Compare one incremental commit with four full SQL GROUP BY scans, checking every cell."""

import argparse
import hashlib
import json
import platform
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from aurora_netview.core import full_recompute, poll, rollups
from aurora_netview.fixtures import DEMO_NOW, append, create, events


def run(warmups=2, repetitions=7):
    observations = []
    with tempfile.TemporaryDirectory(prefix="aurora-benchmark-") as directory:
        directory = Path(directory)
        for history in (10_000, 100_000):
            base_source = directory / f"source-{history}.sqlite"
            base_cache = directory / f"cache-{history}.sqlite"
            create(base_source)
            append(base_source, events(history))
            while poll(
                base_source,
                base_cache,
                source_id="benchmark-v1",
                now=DEMO_NOW,
                batch_size=10_000,
            )["remaining"]:
                pass
            # Snapshot through SQLite's backup API; never copy an open WAL file.
            template = directory / f"template-{history}.sqlite"
            with (
                closing(sqlite3.connect(base_cache)) as src,
                closing(sqlite3.connect(template)) as dst,
            ):
                src.backup(dst)
            for batch in (100, 1000):
                source = directory / f"source-{history}-{batch}.sqlite"
                shutil.copyfile(base_source, source)  # closed fixture, rollback journal
                append(source, events(batch, offset=history))
                for iteration in range(warmups + repetitions):
                    cache = directory / f"trial-{history}-{batch}-{iteration}.sqlite"
                    shutil.copyfile(template, cache)
                    order = (
                        ("incremental", "full")
                        if iteration % 2 == 0
                        else ("full", "incremental")
                    )
                    timings = {}
                    for method in order:
                        start = time.perf_counter_ns()
                        if method == "incremental":
                            outcome = poll(
                                source,
                                cache,
                                source_id="benchmark-v1",
                                now=DEMO_NOW,
                                batch_size=batch,
                            )
                        else:
                            expected = full_recompute(source, now=DEMO_NOW)
                        timings[method + "_ns"] = time.perf_counter_ns() - start
                    actual = rollups(cache)
                    if (
                        actual != expected
                        or outcome["read"] != batch
                        or outcome["remaining"]
                    ):
                        raise AssertionError(
                            "Incremental output differs from full recomputation"
                        )
                    observations.append(
                        {
                            "history_rows": history,
                            "new_rows": batch,
                            "source_rows": history + batch,
                            "iteration": iteration,
                            "warmup": iteration < warmups,
                            "order": list(order),
                            **timings,
                            "exact_match": True,
                            "rollup_cells": {
                                name: len(cells) for name, cells in actual.items()
                            },
                        }
                    )
    summaries = []
    for history in (10_000, 100_000):
        for batch in (100, 1000):
            values = [
                v
                for v in observations
                if not v["warmup"]
                and v["history_rows"] == history
                and v["new_rows"] == batch
            ]
            inc = statistics.median(v["incremental_ns"] for v in values)
            full = statistics.median(v["full_ns"] for v in values)
            summaries.append(
                {
                    "history_rows": history,
                    "new_rows": batch,
                    "incremental_median_ms": inc / 1e6,
                    "full_median_ms": full / 1e6,
                    "full_over_incremental_ratio": full / inc,
                }
            )
    return {
        "schema_version": 1,
        "captured_utc": datetime.now(timezone.utc).isoformat(),
        "host_label": "validation-host-1",
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "warmups_per_case": warmups,
        "measured_repetitions_per_case": repetitions,
        "fixture": {
            "clients": 24,
            "domains": 48,
            "event_span_seconds": 72000,
            "retention_hours": 24,
            "clock": DEMO_NOW,
            "client_domain_mapping": "two domains per client",
        },
        "timing_scope": {
            "incremental": "bounded read, four persistent rollups, retention and atomic cursor commit",
            "full": "four full SQL GROUP BY scans and Python result maps; no persistent writes",
            "excluded": "fixture creation, initial cache build, template copy, correctness comparison, dashboard refresh",
        },
        "source_sha256": {
            p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
            for p in (
                "aurora_netview/core.py",
                "aurora_netview/fixtures.py",
                "benchmarks/run.py",
            )
        },
        "summaries": summaries,
        "observations": observations,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    # Reserve output before measurement, and never overwrite existing evidence.
    with Path(args.out).open("x", encoding="utf-8") as output:
        result = run()
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps(result["summaries"], indent=2))
