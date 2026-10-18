import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from aurora_netview.__main__ import demo
from aurora_netview.core import (
    SourceChanged,
    dashboard,
    full_recompute,
    poll,
    readonly,
    refresh,
    rollups,
)
from aurora_netview.fixtures import DEMO_NOW, append, create, events
from aurora_netview.view import render


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.source = Path(self.tmp.name) / "source.sqlite"
        self.cache = Path(self.tmp.name) / "cache.sqlite"
        self.now = DEMO_NOW
        create(self.source)

    def run_poll(self, **kwargs):
        return poll(
            self.source,
            self.cache,
            source_id=kwargs.pop("source_id", "fixture-v1"),
            now=kwargs.pop("now", self.now),
            **kwargs,
        )

    def equal(self, now=None):
        self.assertEqual(
            rollups(self.cache), full_recompute(self.source, now=now or self.now)
        )

    def test_four_rollups_match_sql_oracle_across_bounded_batches(self):
        append(self.source, events(123))
        sizes = []
        while True:
            result = self.run_poll(batch_size=17)
            sizes.append(result["read"])
            if not result["remaining"]:
                break
        self.assertEqual(sizes, [17] * 7 + [4])
        self.equal()
        self.assertEqual(sum(rollups(self.cache)["hourly_totals"].values()), 123)

    def test_same_timestamp_and_late_arrivals_use_new_ids(self):
        append(self.source, [(self.now - 1, "192.0.2.1", "a.example")])
        self.run_poll()
        append(
            self.source,
            [
                (self.now - 1, "192.0.2.1", "b.example"),
                (self.now - 3600, "192.0.2.2", "a.example"),
            ],
        )
        self.assertEqual(self.run_poll()["read"], 2)
        self.equal()

    def test_empty_poll_updates_health_without_double_count(self):
        append(self.source, events(8))
        self.run_poll()
        before = rollups(self.cache)
        result = self.run_poll(now=self.now + 10)
        self.assertEqual(result["read"], 0)
        self.assertEqual(rollups(self.cache), before)
        self.assertEqual(
            dashboard(self.cache, now=self.now + 10)["poll_age_seconds"], 0
        )

    def test_dashboard_distinguishes_poll_health_from_catchup(self):
        append(self.source, events(20))
        self.run_poll(batch_size=10)
        report = dashboard(self.cache, now=self.now)
        self.assertFalse(report["caught_up_at_poll"])
        self.assertEqual(report["observed_max_id"], 20)
        self.assertEqual(report["queries"], 10)
        self.run_poll(batch_size=10)
        self.assertTrue(dashboard(self.cache, now=self.now)["caught_up_at_poll"])

    def test_expired_late_events_advance_cursor_without_resurrection(self):
        append(self.source, [(self.now - 200000, "192.0.2.1", "old.example")])
        r = self.run_poll()
        self.assertEqual(r["expired_on_arrival"], 1)
        self.assertEqual(r["cursor"], 1)
        self.equal()

    def test_retention_boundary_and_empty_poll_expire_snapshots(self):
        cutoff = self.now // 3600 * 3600 - 24 * 3600
        append(
            self.source,
            [
                (cutoff, "192.0.2.1", "keep.example"),
                (cutoff - 1, "192.0.2.1", "expire.example"),
            ],
        )
        self.run_poll()
        refresh(self.cache, now=self.now)
        self.assertEqual(dashboard(self.cache, now=self.now)["queries"], 1)
        later = self.now + 26 * 3600
        self.run_poll(now=later)
        self.assertEqual(dashboard(self.cache, now=later)["clients"], [])
        self.equal(later)

    def test_exception_before_commit_rolls_back_everything(self):
        append(self.source, events(5))
        self.run_poll()
        before = rollups(self.cache)
        append(self.source, events(3, offset=5))

        def fail():
            raise RuntimeError("injected crash boundary")

        with self.assertRaises(RuntimeError):
            self.run_poll(_before_commit=fail)
        self.assertEqual(rollups(self.cache), before)
        self.assertEqual(self.run_poll()["read"], 3)
        self.equal()

    def test_process_exit_before_commit_replays_exactly_once(self):
        append(self.source, events(10))
        self.run_poll()
        append(self.source, events(5, offset=10))
        code = 'import os,sys; sys.path.insert(0,os.getcwd()); from aurora_netview.core import poll; poll(sys.argv[1],sys.argv[2],source_id="fixture-v1",now=int(sys.argv[3]),_before_commit=lambda:os._exit(73))'
        p = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(self.source),
                str(self.cache),
                str(self.now),
            ],
            capture_output=True,
        )
        self.assertEqual(p.returncode, 73)
        self.assertEqual(dashboard(self.cache, now=self.now)["cursor"], 10)
        self.assertEqual(self.run_poll()["read"], 5)
        self.equal()

    def test_two_processes_serialize_checkpoint_and_counts(self):
        self.run_poll()
        append(self.source, events(200))
        code = 'import os,sys; sys.path.insert(0,os.getcwd()); from aurora_netview.core import poll; poll(sys.argv[1],sys.argv[2],source_id="fixture-v1",now=int(sys.argv[3]),batch_size=100)'
        processes = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    code,
                    str(self.source),
                    str(self.cache),
                    str(self.now),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            for _ in range(2)
        ]
        for process in processes:
            out, err = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, err.decode())
        self.assertEqual(dashboard(self.cache, now=self.now)["processed"], 200)
        self.equal()

    def test_identity_change_and_retention_change_fail_closed(self):
        self.run_poll()
        with self.assertRaises(SourceChanged):
            self.run_poll(source_id="replacement-v2")
        with self.assertRaises(SourceChanged):
            self.run_poll(retention_hours=48)

    def test_source_id_regression_or_checkpoint_deletion_rejected(self):
        append(self.source, events(3))
        self.run_poll()
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("DELETE FROM query_storage WHERE id=3")
        with self.assertRaises(SourceChanged):
            self.run_poll()

    def test_checkpoint_mutation_rejected(self):
        append(self.source, events(3))
        self.run_poll()
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("UPDATE query_storage SET timestamp=timestamp-1 WHERE id=3")
        with self.assertRaises(SourceChanged):
            self.run_poll()

    def test_older_mutation_requires_full_verification(self):
        append(
            self.source,
            [
                (self.now - 1, "192.0.2.1", "a.example"),
                (self.now - 2, "192.0.2.2", "b.example"),
            ],
        )
        self.run_poll()
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("UPDATE query_storage SET domain=2 WHERE id=1")
        self.run_poll()  # explicitly documented fast-path limitation
        self.assertNotEqual(
            rollups(self.cache), full_recompute(self.source, now=self.now)
        )

    def test_future_event_rejects_whole_batch(self):
        append(
            self.source,
            [
                (self.now - 1, "192.0.2.1", "ok.example"),
                (self.now + 1, "192.0.2.1", "future.example"),
            ],
        )
        with self.assertRaises(ValueError):
            self.run_poll()
        with closing(sqlite3.connect(self.cache)) as c, c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM state").fetchone()[0], 0)
            self.assertEqual(
                c.execute("SELECT COUNT(*) FROM hourly_totals").fetchone()[0], 0
            )

    def test_readonly_source_is_unchanged_and_rejects_writes(self):
        append(self.source, events(8))
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.run_poll()
        self.equal()
        c = readonly(self.source)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                c.execute("DELETE FROM query_storage")
        finally:
            c.close()
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), before)

    def test_missing_source_does_not_create_it(self):
        missing = Path(self.tmp.name) / "absent.sqlite"
        with self.assertRaises(sqlite3.OperationalError):
            poll(missing, self.cache, source_id="missing", now=self.now)
        self.assertFalse(missing.exists())

    def test_same_file_and_hardlink_alias_rejected(self):
        with self.assertRaises(ValueError):
            poll(self.source, self.source, source_id="x", now=self.now)
        try:
            os.link(self.source, self.cache)
        except OSError:
            self.skipTest("hardlinks unavailable")
        with self.assertRaises(ValueError):
            self.run_poll()

    def test_missing_schema_and_join_dimension_rejected(self):
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("DROP TABLE client_by_id")
        with self.assertRaises(ValueError):
            self.run_poll()

    def test_missing_dimension_does_not_skip_ids(self):
        append(self.source, events(2))
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("DELETE FROM domain_by_id")
        with self.assertRaises(ValueError):
            self.run_poll()

    def test_legacy_string_references_fail_clearly(self):
        append(self.source, events(1))
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("UPDATE query_storage SET domain='legacy.example'")
        with self.assertRaisesRegex(ValueError, "Legacy"):
            self.run_poll()

    def test_fair_refresh_budget_and_unchanged_poll_no_churn(self):
        append(
            self.source,
            [(self.now - 1, f"192.0.2.{i}", "a.example") for i in range(1, 6)],
        )
        self.run_poll()
        order = []
        for n in range(5):
            self.run_poll(now=self.now + n)
            order.extend(refresh(self.cache, now=self.now + n, limit=1))
        self.assertEqual(len(set(order)), 5)
        self.assertEqual(refresh(self.cache, now=self.now + 10, limit=5), [])
        self.assertEqual(refresh(self.cache, now=self.now + 2000, limit=1), [order[0]])

    def test_dirty_snapshot_and_expiry_are_visible(self):
        append(self.source, events(1))
        self.run_poll()
        refresh(self.cache, now=self.now)
        self.assertFalse(dashboard(self.cache, now=self.now)["clients"][0]["stale"])
        self.assertTrue(
            dashboard(self.cache, now=self.now + 1800)["clients"][0]["stale"]
        )
        append(self.source, [(self.now - 1, "192.0.2.1", "new.example")])
        self.run_poll()
        self.assertTrue(dashboard(self.cache, now=self.now)["clients"][0]["stale"])
        refresh(self.cache, now=self.now + 1)
        self.assertFalse(dashboard(self.cache, now=self.now + 1)["clients"][0]["stale"])

    def test_backward_clock_and_invalid_bounds_rejected(self):
        self.run_poll()
        for kwargs in (
            {"now": self.now - 1},
            {"batch_size": 0},
            {"batch_size": 100001},
            {"retention_hours": 0},
        ):
            with self.assertRaises(ValueError):
                self.run_poll(**kwargs)

    def test_refresh_clock_remains_monotonic_even_without_due_clients(self):
        append(self.source, events(2))
        self.run_poll()
        refresh(self.cache, now=self.now + 100)
        with self.assertRaises(ValueError):
            refresh(self.cache, now=self.now + 10)
        with self.assertRaises(ValueError):
            self.run_poll(now=self.now + 10)
        self.assertEqual(refresh(self.cache, now=self.now + 200), [])
        with self.assertRaises(ValueError):
            refresh(self.cache, now=self.now + 150)
        self.assertTrue(
            all(
                c["refreshed"] == self.now + 100
                for c in dashboard(self.cache, now=self.now + 200)["clients"]
            )
        )

    def test_nonpositive_source_ids_rejected(self):
        append(self.source, events(1))
        with closing(sqlite3.connect(self.source)) as c, c:
            c.execute("UPDATE query_storage SET id=0")
        with self.assertRaisesRegex(ValueError, "positive"):
            self.run_poll()

    def test_dashboard_escapes_source_labels(self):
        append(self.source, [(self.now - 1, "<script>bad</script>", "<img src=x>")])
        self.run_poll()
        refresh(self.cache, now=self.now)
        output = render(dashboard(self.cache, now=self.now))
        self.assertNotIn("<script>", output)
        self.assertIn("&lt;script&gt;", output)
        self.assertNotIn("<img src=x>", output)

    def test_continuously_dirty_client_does_not_starve_other_clients(self):
        append(
            self.source,
            [(self.now - 1, f"192.0.2.{i}", "a.example") for i in range(1, 6)],
        )
        self.run_poll()
        order = []
        for n in range(5):
            append(self.source, [(self.now - 1, "192.0.2.1", "hot.example")])
            self.run_poll(now=self.now + n)
            order.extend(refresh(self.cache, now=self.now + n, limit=1))
        self.assertEqual(len(set(order)), 5)

    def test_demo_is_reproducible_and_refuses_existing_output(self):
        first, second = Path(self.tmp.name) / "demo-1", Path(self.tmp.name) / "demo-2"
        a, b = demo(first), demo(second)
        self.assertEqual(a, b)
        self.assertEqual(a["queries"], 1201)
        self.assertEqual(a["cursor"], 1202)
        self.assertEqual(a["expired_on_arrival"], 1)
        self.assertEqual(
            (first / "index.html").read_bytes(), (second / "index.html").read_bytes()
        )
        with self.assertRaises(FileExistsError):
            demo(first)


if __name__ == "__main__":
    unittest.main()
