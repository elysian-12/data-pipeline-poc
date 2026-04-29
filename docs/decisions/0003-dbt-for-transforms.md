# ADR 0003 — dbt-core for all silver→gold transformations

**Status**: Accepted
**Date**: 2026-04-19

## Context

Silver → gold needs: typed column contracts, tests (unique, not_null,
relationships, range), incremental materializations, lineage graph, and
browsable docs.

Candidates: raw SQL executed by Python, SQLAlchemy Core, dbt-core, SQLMesh.

## Decision

**dbt-core + dbt-duckdb** for all silver→gold SQL. Python is responsible for
ingest + silver MERGE + analysis; dbt is responsible for the star schema and
its tests.

- Profile: `data_platform_poc` with `duckdb` target (default) and a
  commented-out `clickhouse` target (see
  [ADR 0001](0001-duckdb-default-clickhouse-scale.md)).
- Models: `silver/` (views over the Python-owned silver layer) → `gold/` (dim + fact tables). Directory names mirror the medallion layer they represent. The `silver/` dir's views materialize into a `staging` schema to avoid colliding with the Python-owned `silver.stg_prices` table.
- Tests live in `schema.yml` + a singular test for calendar gap coverage.
- Seeds: `asset_catalog.csv` — asset metadata enrichment.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Raw `.sql` executed by Python** | Re-implements what dbt gives for free: lineage, tests, docs, incremental materialization syntax, schema.yml contracts. |
| **SQLAlchemy Core** | ORM overhead for no benefit — we do not do per-row CRUD, we do analytical SQL. SQLAlchemy's SQL expression language is verbose vs. dbt's Jinja + native SQL. |
| **SQLMesh** | Interesting alternative (better virtual data environments + semantic versioning), but smaller community and a learning-curve cost that's hard to justify at 3k rows. dbt is the industry default; `ref('stg_prices')` is recognisable without explanation. |

## Consequences

**Positive**

- `dbt test` runs as part of `make run` — DQ failures fail the pipeline.
- `dbt docs generate && dbt docs serve` produces a clickable lineage graph
  that's an asset for the written analysis.
- Adapter swap to ClickHouse = one profile change. Model SQL is portable.
- `models/gold/fact_daily_price.sql` is self-describing — the grain is
  obvious from the SQL.

**Negative**

- Adds a dependency (`dbt-core`, `dbt-duckdb`) — Python start-up cost.
- DuckDB single-writer lock means `dbt run` cannot parallelize across
  threads; we set `threads: 1`. At scale (ClickHouse) this limit disappears.
- dbt Jinja is a templating language — complex logic is harder to read than
  Python. We keep transformations declarative; any non-trivial logic lives in
  `src/analysis/`.

## Cost to reverse

**Medium.** Moving off dbt means re-implementing: the lineage graph, the
tests (unique, not_null, accepted_range, singular), `ref()` / `source()`
resolution, incremental materialization helpers. About 200–300 lines of
Python. Unlikely to be worth it unless the project has an explicit non-SQL
transform layer (Spark, Flink).
