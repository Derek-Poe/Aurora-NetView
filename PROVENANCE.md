# Provenance and scope

Aurora NetView began as my local DNS activity dashboard. Its original aggregator materialized four hourly count tables from Pi-hole's normalized query storage; its client summarizer built per-client activity snapshots and optionally requested model-generated interpretation.

The reviewed original source snapshot is commit `61d963dd400d6fb2bd8ae2b7d7336b7dc4ba8f19`. The original deployment remains separate and unchanged. This repository is a new, focused implementation informed by that source, not a claim that the original deployment already had the new reliability properties.

| Original relative source | SHA-256 of reviewed bytes | Contribution retained |
| --- | --- | --- |
| `src/pihole_stats_aggregator.py` | `f1f27b4f7f39c0d861880e9933ba21d67d94a727beca2d258994c6eee95ae082` | Read-only normalized Pi-hole queries; four hourly materialized views; SQLite WAL and retention. |
| `src/pihole_client_summarizer.py` | `c764b664cc90aed2791f1c0c90dc08051ac0784ea4ec9b0b7f18baaafb66f497` | Cached client activity and top-domain summaries; separate refresh work. |

The portfolio implementation adds stable source-ID checkpointing, one transaction for counts/checkpoint/expiry, explicit reset and mutation semantics, bounded input batches, monotonic clocks, deterministic refresh fairness, a generated schema fixture, an offline dashboard, failure tests, and exact-output benchmark evidence. It replaces timestamp-based checkpointing and model-dependent scheduling rather than presenting those old mechanisms as proven reliable.

The original Flask application, optional inference integration, operational configuration, real client/domain histories, and character/media assets are excluded. There are no third-party runtime libraries or copied third-party source files in this subset. The [Pi-hole database documentation](https://docs.pi-hole.net/database/query-database/) supplies the adapter's supported schema contract; Pi-hole is not bundled. Python and its SQLite module are supplied by the user's Python installation.

Synthetic addresses use the documentation-only `192.0.2.0/24` block and domains end in `.example`. New code, tests, documentation, and generated fixtures are released by Derek Poe under MIT.
