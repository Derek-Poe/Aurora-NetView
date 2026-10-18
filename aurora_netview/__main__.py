import argparse
import json
import time
from pathlib import Path

from .core import dashboard, full_recompute, poll, refresh, rollups
from .fixtures import DEMO_NOW, append, create, events
from .view import render


def demo(out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    source, cache = out / "source.sqlite", out / "cache.sqlite"
    create(source)
    append(source, events(1200))
    batches = []
    while True:
        result = poll(
            source, cache, source_id="synthetic-demo-v1", now=DEMO_NOW, batch_size=100
        )
        batches.append(result)
        if not result["remaining"]:
            break
    # Late event with an existing timestamp is picked up by its new source ID.
    append(
        source,
        [
            (DEMO_NOW - 1, "192.0.2.1", "late.example"),
            (DEMO_NOW - 200000, "192.0.2.1", "expired.example"),
        ],
    )
    batches.append(
        poll(source, cache, source_id="synthetic-demo-v1", now=DEMO_NOW, batch_size=100)
    )
    order = []
    for _ in range(6):
        order.extend(refresh(cache, now=DEMO_NOW, limit=4))
    assert rollups(cache) == full_recompute(source, now=DEMO_NOW)
    report = dashboard(cache, now=DEMO_NOW)
    report.update(
        {
            "synthetic": True,
            "exact_full_recompute_match": True,
            "batches": batches,
            "refresh_order": order,
        }
    )
    (out / "report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    (out / "index.html").write_text(render(report), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Read-only normalized Pi-hole ingestion; synthetic demo available"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    d = commands.add_parser("demo")
    d.add_argument("--out", required=True)
    for name in ("ingest", "verify"):
        p = commands.add_parser(name)
        p.add_argument("--source", required=True)
        p.add_argument("--cache", required=True)
        p.add_argument("--now", type=int, default=None)
        p.add_argument("--retention-hours", type=int, default=24)
        if name == "ingest":
            p.add_argument("--source-id", required=True)
            p.add_argument("--batch-size", type=int, default=1000)
    r = commands.add_parser("refresh")
    r.add_argument("--cache", required=True)
    r.add_argument("--limit", type=int, default=20)
    r.add_argument("--now", type=int, default=None)
    v = commands.add_parser("view")
    v.add_argument("--cache", required=True)
    v.add_argument("--out", required=True)
    v.add_argument("--now", type=int, default=None)
    args = parser.parse_args()
    now = getattr(args, "now", None)
    if now is None:
        now = int(time.time())
    if args.command == "demo":
        print(json.dumps({"directory": args.out, "queries": demo(args.out)["queries"]}))
    elif args.command == "ingest":
        print(
            json.dumps(
                poll(
                    args.source,
                    args.cache,
                    source_id=args.source_id,
                    now=now,
                    batch_size=args.batch_size,
                    retention_hours=args.retention_hours,
                )
            )
        )
    elif args.command == "refresh":
        print(json.dumps(refresh(args.cache, now=now, limit=args.limit)))
    elif args.command == "verify":
        if rollups(args.cache) != full_recompute(
            args.source, now=now, retention_hours=args.retention_hours
        ):
            raise SystemExit(
                "Mismatch: source changed, cache incomplete, or verification clock/window differs"
            )
        print("All four rollups exactly match full recomputation")
    else:
        report = dashboard(args.cache, now=now)
        # Exclusive output avoids overwriting a source/cache or existing artifact.
        with Path(args.out).open("x", encoding="utf-8") as output:
            output.write(render(report))


if __name__ == "__main__":
    main()
