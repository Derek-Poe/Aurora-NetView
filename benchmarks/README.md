# Incremental ingestion versus full recomputation

Run from the repository root:

```sh
python benchmarks/run.py --out benchmark-new.json
```

The script creates fresh synthetic sources and cache snapshots in a temporary directory. It reserves a new output file and refuses to overwrite existing results. [Raw observations](results.json) include the environment, code hashes, execution order, every timing, and every exact-match result.

## Recorded experiment

Windows, Python 3.14.0, SQLite version recorded in the JSON; neutral host label `validation-host-1`. Other portfolio build/test workers were paused for this run. Ordinary background OS activity was not disabled. Each case has two warmups and seven measured repetitions, with method order alternating. Every repetition starts from an equivalent initialized cache; the source contains the same historical rows plus the stated new batch.

| Existing rows | New rows | Incremental median | Full recompute median | Full / incremental |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 100 | 8.59 ms | 17.47 ms | 2.03x |
| 10,000 | 1,000 | 23.65 ms | 19.07 ms | 0.81x |
| 100,000 | 100 | 9.75 ms | 232.86 ms | 23.87x |
| 100,000 | 1,000 | 35.71 ms | 339.25 ms | 9.50x |

All **36 observations**, including eight warmups, matched every key/count in all four rollups. The incremental path was slower for the 10,000-row history plus 1,000-row batch: transaction and per-row update costs outweighed the work avoided. The larger-history cases show why retaining rollups can help when new input is small relative to accumulated history.

## What the timings mean

The incremental timer includes source connection/schema checks, checkpoint validation, reading the new batch, four persistent rollup updates, retention checks, dirty-client bookkeeping, and the atomic cursor commit using SQLite WAL and `synchronous=FULL`. The full baseline independently executes four SQL `GROUP BY` scans and constructs Python maps; **it does not persist its result**. This comparison is deliberately explicit about that asymmetry. It is not a comparison with an optimized database engine's native incremental view maintenance.

Fixture creation, initial cache population, cache-template copies, output comparison, dashboard rendering, and client-summary refresh are outside both timers. OS filesystem caches are warm/uncontrolled; this is not a cold-cache test. Cache snapshots use SQLite's backup API before trial files are copied, avoiding inconsistent copies of an active WAL database.

The fixture has 24 clients, 48 domains (two per client), and events spread over 20 hours, all inside a 24-hour retention window. It exercises late event-time order but no expiration storm, high-cardinality join explosion, active source writer, or simultaneous pollers during timing. Contention and failure behavior are tested separately. Four small synthetic cases on one host do not establish production throughput, tail latency, a scaling law, or a universal speedup. Larger cardinalities, retained-history pruning, storage latency, and refresh budgets would need workload-specific measurements before deployment.
