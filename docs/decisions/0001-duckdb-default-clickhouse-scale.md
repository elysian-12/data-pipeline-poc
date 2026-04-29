# ADR 0001 — DuckDB by default; ClickHouse at scale

**Status**: Accepted
**Date**: 2026-04-19

## Context

The warehouse needs to hold silver (staging) + gold (star schema) for ~3,000
fact rows from 8 assets × 365 days. The design must justify its choice in
both the "right now" and "5 orders of magnitude from now" regimes.

Candidate engines: PostgreSQL, DuckDB, ClickHouse, BigQuery / Snowflake.

## Decision

**DuckDB** (embedded, single-process, columnar OLAP) for the demo;
**ClickHouse** (distributed columnar OLAP) documented as the production scale
path.

DuckDB is the `default` dbt target. ClickHouse is a commented-out target in
`dbt/profiles.yml`. dbt models are adapter-neutral — standard incremental
materializations, window functions, `QUALIFY` — both adapters support them.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **PostgreSQL** | Row store — OLAP window functions over 30k+ rows will be 10–100× slower than columnar. Would need to migrate anyway at scale. |
| **ClickHouse now** | Requires Docker, Keeper, shards, replicas. At 3k rows it is theatre — the local machine becomes a cluster operator. Demonstrates no additional pattern over DuckDB + dbt. |
| **BigQuery / Snowflake** | Requires a cloud account + cost to reproduce the demo. Violates zero-friction local setup. |

## Consequences

**Positive**

- Clone → `uv sync` → `make init`. No daemons, no Docker.
- DuckDB + dbt is the same pattern that scales: swap adapter + engine.
- Shared SQL dialect means `MERGE`, window functions, incremental models port
  cleanly.

**Negative**

- DuckDB single-writer lock constrains us to serial execution within one
  process (mitigated: we serialize by design — ingest → dbt → analyze are
  sequential anyway).
- DuckDB's `ON CONFLICT` syntax differs slightly from ClickHouse's
  `ReplacingMergeTree(ingested_at)` — the MERGE helper in
  `pipeline/storage/warehouse.py` contains DuckDB-specific SQL. A ClickHouse
  port requires swapping that helper (documented).

## Cost to reverse

**Low.** Migration recipe:

1. `dbt/profiles.yml`: activate the `clickhouse` target.
2. Convert `silver.stg_prices` DDL from `PRIMARY KEY (symbol, date)` to
   `ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (symbol, date)`.
3. Rewrite the MERGE helper: insert new rows + `OPTIMIZE TABLE ... FINAL` to
   merge by `ingested_at` (last-write-wins semantics preserved).
4. Type alignment: `DOUBLE` → `Float64`, `TIMESTAMP WITH TIME ZONE` → `DateTime64(9, 'UTC')`, `VARCHAR` → `String`.
5. Gold models: zero changes — dbt-clickhouse supports `materialized='incremental'` with `unique_key`.

No Python changes outside the warehouse helper. No analysis changes.
