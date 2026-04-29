# ADR 0002 — Bronze on Parquet via fsspec; S3-ready by URI

**Status**: Accepted
**Date**: 2026-04-19

## Context

Bronze is the immutable source of truth — the thing we replay from when silver
or gold is corrupted. Candidates: raw JSON blobs, partitioned Parquet, a row
store, a document store.

Two constraints shaped this:

1. The pipeline must run locally with zero infrastructure setup.
2. The same code must produce a prod-grade layout (S3 + Hive partitioning)
   without rewriting ingestion.

## Decision

**Partitioned Parquet** written via `fsspec` URI, with the backend chosen
purely by configuration.

- **Local default**: `BRONZE_URI=./data/bronze/` — writes to the filesystem.
- **S3 in prod**: `BRONZE_URI=s3://my-bucket/bronze/` + AWS creds via env.
- **S3-compatible in CI/staging**: same URI + `S3_ENDPOINT=http://minio:9000`.

`pyarrow.parquet.write_table` + `fsspec.filesystem(uri)` handle all three
without an `if` branch in our code.

**Path shape**: `{BRONZE_URI}/source=X/asset_type=Y/ingested_date=Z/data.parquet`
— Hive-style `key=value` so Athena / Redshift Spectrum / ClickHouse `s3()`
engine can read it natively with no manifest.

**Idempotent by overwrite**: the path is deterministic per
`(source, asset_type, ingested_date)`. Re-running the same day's ingest
overwrites the single file. Lineage (`ingested_at`, `run_id`) lives as
**in-row columns** — audit is preserved, storage is bounded by
`sources × asset_types × days` (~1,100 files/year), not by invocation count.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Raw JSON per response** | No schema enforcement; 10× larger; every consumer has to parse. At replay time we don't need the raw bytes — the pydantic models are the schema contract. |
| **Append per invocation (`batch_<ulid>.parquet`)** | Unbounded storage for idempotent data; 10 retries today = 10 files forever. User flagged this explicitly. |
| **Append-only, dedup in silver only** | Works, but doesn't match S3 cost expectations at scale (list cost scales with object count). Overwrite-by-path is cheaper. |
| **Write directly to silver, skip bronze** | No replay isolation — a silver bug means re-hitting the APIs. Bronze is the cheap insurance against transform regressions. |
| **Delta / Iceberg tables** | Excellent at scale; over-engineered for a demo; requires a catalog. When we *do* outgrow Parquet, migrating to Iceberg is additive — the files stay, we just add a metadata layer. |

## Consequences

**Positive**

- Zero-setup local run; zero-code prod flip.
- Re-ingest is always safe — no duplicate files, no silver dupes (MERGE
  handles it).
- Every downstream consumer (DuckDB, ClickHouse, Athena, Spark) reads
  partitioned Parquet natively.
- Schema enforcement at write time via pydantic → Arrow.

**Negative**

- Overwriting a day loses the *previous attempt's* bytes. We trade
  per-attempt immutability for bounded storage. The per-invocation audit
  trail lives in `meta.pipeline_runs` + the in-row `run_id` of the
  **winning** attempt — good enough for this scale.
- If two cron runs ever overlapped for the same `ingested_date`, the last
  writer wins silently. Mitigation: DuckDB single-writer lock serializes us
  anyway; at prod scale we'd add a run lock in `meta.pipeline_runs`.

## Cost to reverse

**Low for "add per-invocation immutability"**: flip the path to
`source=X/asset_type=Y/ingested_date=Z/batch_<run_id>.parquet` and add a
nightly compaction job. Bronze readers downstream don't care — they glob the
partition.

**Low for "migrate to Iceberg/Delta"**: existing Parquet files remain valid
data; add a table metadata layer on top.
