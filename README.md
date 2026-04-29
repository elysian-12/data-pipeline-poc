# data-pipeline-poc — Traditional Assets vs Bitcoin

A Python data pipeline that compares traditional assets (FX, equities, S&P 500
proxy) against Bitcoin over the last 365 days. Ingests from
[Massive](https://massive.com/docs) and
[CoinGecko](https://docs.coingecko.com/reference/introduction), lands the data
in a medallion warehouse, and produces a written analysis covering
winners per window, lump-sum vs DCA into BTC, and fiat-vs-BTC volatility.

The dataset is tiny (~3,000 fact rows), but the pipeline is built to
demonstrate patterns that scale five orders of magnitude without rewrites:
object-store bronze, columnar warehouse, star schema, idempotent upserts,
dbt-managed lineage, typed boundaries, and a full test suite.

---

## At a glance

|                       |                                                                                                                                                                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Run one thing**     | `make run` — incremental ingest → transform → analyze (see [Quickstart](#quickstart))                                                                                                                                |
| **Answers**           | [`DATA_REPORTS/data_analysis.html`](DATA_REPORTS/) (visual panel) + [`DATA_REPORTS/data_analysis.md`](DATA_REPORTS/) (text summary) — tables land in [`outputs/`](outputs/)                                          |
| **Data flow**         | Bronze Parquet → DuckDB Silver (MERGE) → dbt Gold (star schema)                                                                                                                                                      |
| **Ingestion cadence** | `make init` once on fresh clones (bootstrap + run); `make run` for recurring ingest → transform → analyze; install daily 02:00 UTC cron with `make schedule` (cron calls `make run`) — see [Scheduling](#scheduling) |
| **Storage**           | Local by default; `BRONZE_URI=s3://…` flips to S3 — no code change                                                                                                                                                   |
| **Warehouse**         | DuckDB embedded; ClickHouse port documented in [ADR 0001](docs/decisions/0001-duckdb-default-clickhouse-scale.md)                                                                                                    |

---

## Quickstart

Prereqs: Python 3.12+, [`uv`](https://github.com/astral-sh/uv) (`brew install uv`).

**One-time setup**

- `uv sync` — install deps
- `cp .env.example .env` — fill in `MASSIVE_API_KEY`
- `source .venv/bin/activate`
- `uv run dbt deps --project-dir dbt --profiles-dir dbt` — install `dbt_utils`

**Run the pipeline**

_From scratch (first time or reproducibility check)_

- `make clean && make init` — nuke all artifacts (warehouse, bronze, outputs), bootstrap the warehouse, then run the full pipeline. Ingests the default 365-day lookback.

_Incremental run (daily / after a gap)_

- `make run` — ingest (from `max(silver.date)+1` → yesterday-UTC) → dbt → analyze. **No bootstrap** — schema DDL + calendar seed already landed during `make init`. Safe to re-run any time; MERGE on `(symbol, date)` keeps silver dup-free. This is what cron runs nightly at 02:00 UTC.

_Backfill run (explicit historical range)_

- `make backfill START=2024-01-01 END=2024-06-30` — fetch an explicit window. Same MERGE, so re-running overlapping ranges is a no-op.
- `make backfill-gaps` — scans `fact_daily_price` for missing trading days and refills them automatically.

**Validate**

- `make test` — the full local gate: `ruff check` + `mypy --strict` + `pytest` + `dbt parse`
- `make lint` — `ruff check` + `ruff format --check`
- `make typecheck` — `mypy --strict` on `src/`
- `make ci` — what CI runs: `uv sync --extra dev` then `make test`
- dbt tests run automatically inside `make run` / `make init` (42 schema tests + 1 singular; pipeline fails if any fails — see [Failure handling](#failure-handling))

**Inspect**

- `make doctor` — recent runs, freshness per symbol, DQ failures, orphan `running` rows
- Open [`DATA_REPORTS/data_analysis.html`](DATA_REPORTS/) for the visual panel

**Outputs**

Tabular outputs land in [`outputs/`](outputs/):

- `returns_by_window.{csv,parquet}` — answers **Q1** (asset returns 1Y/YTD/6M/3M/1M/7D, flagged if > BTC)
- `lump_sum_1k.{csv,parquet}` — answers **Q2** ($1,000 invested 1Y ago)
- `dca_vs_lump.{csv,parquet}` — answers **Q3** ($100/mo × 12 DCA vs $1,200 lump)
- `volatility_summary.{csv,parquet}` — answers **Q4** (fiat vs BTC volatility)
- `correlation_matrix.{csv,parquet}` — Pearson on inner-joined dates

Narrative reports land in [`DATA_REPORTS/`](DATA_REPORTS/) at the repo root (separate from the machine-readable tables in `outputs/`):

- `data_analysis.md` — text summary with the 4 answers
- `data_analysis.html` — visual panel (plotly loaded from CDN; ~700 KB, renders on GitHub, requires internet): % change lines, grouped-bar window winners, growth-of-$1k, DCA-vs-lump BTC time series, rolling annualised volatility, risk-return scatter, correlation heatmap, each paired with a short storytelling paragraph
- `data_analysis_static.html` — same panel with plotly.js inlined (~5 MB); self-contained static build that works without network
- `performance_report.html` — per-stage wall-time and peak-RSS panel rendered from [`outputs/performance.jsonl`](outputs/) (one record per `timed()` block); shows the latest run plus a sparkline of the last N runs so regressions surface visually

---

## Core assumption — USD is the numéraire

**Every price is USD-denominated, and `1 USD ≡ $1.00` by construction.**
USD is seeded in silver as a `source='synthetic'` row with `close=1.0` —
there is no `USDUSD` ticker. BTC is USD per 1 BTC (CoinGecko
`vs_currency=usd`); `C:EURUSD` is USD per 1 EUR; US equities are
USD-native. Every `base_ccy` in
[config/settings.yaml](config/settings.yaml) is `USD`.

Consequences to read the outputs correctly:

- USD's "return" is exactly 0% and its volatility is 0 — **true by
  definition, not measurement**. Nominal only; no inflation adjustment.
- FX returns are USD-relative: `C:EURUSD +5%` means EUR appreciated vs USD,
  not in some absolute frame.
- A non-USD-quoted pair (e.g. `C:USDJPY`, which returns JPY per 1 USD on
  Massive) would need explicit inversion. Currently not present;
  `dim_asset.base_ccy` exists as a column precisely so this assumption is
  inspectable, not hard-coded.

---

## Architecture

```
┌───────────────────┐       ┌───────────────────┐
│   Massive API     │       │   CoinGecko API   │
│  (FX/Stocks/SPY)  │       │  (BTC close+vol)  │
└─────────┬─────────┘       └─────────┬─────────┘
          │  httpx async + tenacity   │
          ▼  per-provider semaphore   ▼
     ┌────────────────────────────────────┐
     │  src/pipeline/ingest/              │
     │  pydantic v2 response validation   │
     └────────────────┬───────────────────┘
                      ▼
     ┌────────────────────────────────────┐
     │  BRONZE  (Parquet, immutable)      │
     │  fsspec URI → local FS or s3://    │
     │  source=X/asset_type=Y/            │
     │    ingested_date=Z/data.parquet    │
     └────────────────┬───────────────────┘
                      ▼
     ┌────────────────────────────────────┐
     │  DuckDB  —  SILVER + GOLD          │
     │  silver.stg_prices  (MERGE on      │
     │    (symbol,date); ingested_at      │
     │    last-write-wins)                │
     │                                    │
     │  gold.* (dbt-duckdb)               │
     │  dim_asset, dim_asset_type,        │
     │  dim_date, fact_daily_price,       │
     │  fact_daily_metrics                │
     └────────────────┬───────────────────┘
                      ▼
     ┌────────────────────────────────────┐
     │  src/analysis/ (polars)            │
     │  thin gold consumer:               │
     │    fact_daily_metrics → returns,   │
     │    correlation, volatility         │
     │    fact_daily_price  → DCA, lump   │
     └────────────────┬───────────────────┘
                      ▼
            outputs/*.csv + *.parquet
            DATA_REPORTS/data_analysis.md + data_analysis.html
```

Full C4 context + container diagrams live in
[docs/architecture.md](docs/architecture.md).

### HTTP retry policy

Every API call goes through [`ProviderClient.get_json`](src/pipeline/ingest/client.py) — tenacity retries on 429 / 5xx / connection errors with **exponential backoff + jitter (1s → 30s)**, capped at **3 attempts max** (configurable per source via `sources.<name>.max_retries` in [config/settings.yaml](config/settings.yaml); default = 2 retries + 1 initial). 429s honour the `Retry-After` header before the retry.

Synthetic sources (USD base currency) live in [src/pipeline/ingest/synthetic.py](src/pipeline/ingest/synthetic.py) — no HTTP, no retries, deterministic row emission.

### Atomicity guarantees

Each layer commits atomically; a crash anywhere leaves the warehouse
coherent and `make run` recovers on re-run.

| Layer          | Guarantee                                           | Mechanism                                                                                                              |
| -------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **Bronze**     | Full new Parquet visible or old one — never partial | Tmp-write then `fs.mv` (atomic on POSIX; atomic-from-reader on S3). Crash cleanup removes the tmp.                     |
| **Silver**     | MERGE is all-or-nothing per batch                   | Explicit `BEGIN/COMMIT` around `INSERT … ON CONFLICT`.                                                                 |
| **Gold (dbt)** | Per-model atomic                                    | dbt-duckdb wraps each merge in a transaction; `fact_daily_metrics` is a full rebuild.                                  |
| **Meta**       | Every invocation has a row, even on `SIGKILL`       | `track_run` inserts `running`, updates to `success`/`failed` on exit. Orphan `running` rows surface via `make doctor`. |

No single distributed transaction spans the layers. Each stage is
idempotent (deterministic paths, MERGE on `(symbol,date)`, `ingested_at`
last-write-wins), so re-running recovers from any mid-pipeline failure.

---

## API endpoints used

Every upstream call — two endpoints plus Massive pagination. Base URLs are
overridable via [`config/settings.yaml`](config/settings.yaml). Code:
[`ingest/massive.py`](src/pipeline/ingest/massive.py),
[`ingest/coingecko.py`](src/pipeline/ingest/coingecko.py).

| Source                                             | Method & path                                          | Query params                                                                | Auth                                     | Used for                                                                                                |
| -------------------------------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Massive** (`https://api.massive.com`)            | `GET /v2/aggs/ticker/{symbol}/range/1/day/{from}/{to}` | `adjusted=true`, `sort=asc`, `limit=50000`                                  | `Authorization: Bearer $MASSIVE_API_KEY` | Daily OHLCV for US equities (AAPL, GOOGL, MSFT, SPY) and FX (`C:EURUSD`, `C:GBPUSD`)                    |
| **Massive** (pagination)                           | `GET {next_url.path}`                                  | Copied from `next_url`; api key re-appended client-side (Massive strips it) | Same Bearer header                       | Continuation when `next_url` is set                                                                     |
| **CoinGecko** (`https://api.coingecko.com/api/v3`) | `GET /coins/bitcoin/market_chart/range`                | `vs_currency=usd`, `from={unix_ts}`, `to={unix_ts}`                         | None (public free tier)                  | BTC daily close + volume; no OHL (free-tier limit → `dim_asset.price_completeness='close_volume_only'`) |

All quotes resolve to USD — see
[Core assumption](#core-assumption--usd-is-the-numéraire) for why that's
load-bearing.

---

## Common operations

Full reference for every `make` target. For the three run scenarios (from-scratch / incremental / backfill) see [Quickstart](#quickstart).

| Command                        | What it does                                                        |
| ------------------------------ | ------------------------------------------------------------------- |
| `make run`                     | Full pipeline (incremental ingest → dbt → analyze)                  |
| `make ingest`                  | Ingest only                                                         |
| `make transform`               | `dbt run` + `dbt test`                                              |
| `make analyze`                 | Compute the 5 output tables + reports                               |
| `make backfill START= END=`    | Explicit historical range                                           |
| `make backfill-gaps`           | Auto-detect + refill missing trading days                           |
| `make doctor`                  | Health check — recent runs, freshness, DQ failures, suggested fixes |
| `make test`                    | `ruff` + `mypy` + `pytest` + `dbt parse`                            |
| `make lint` / `make typecheck` | `ruff check` + format check / `mypy --strict`                       |
| `make docs-dbt`                | `dbt docs generate && dbt docs serve`                               |
| `make clean`                   | Remove `data/`, `outputs/`, `logs/`, `dbt/target/`, caches          |

---

## Design decisions & trade-offs

Full records in [docs/decisions/](docs/decisions/).

| Choice                                                                                                         | Why                                                                                                     |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **DuckDB primary, ClickHouse documented** ([ADR 0001](docs/decisions/0001-duckdb-default-clickhouse-scale.md)) | Zero-infra at 3k rows; ~95% SQL overlap means the port is a profile swap.                               |
| **Bronze Parquet over fsspec** ([ADR 0002](docs/decisions/0002-bronze-parquet-fsspec-s3-ready.md))             | `BRONZE_URI` swaps local FS ↔ `s3://…` with no code change; deterministic paths make replay idempotent. |
| **dbt-core for transforms** ([ADR 0003](docs/decisions/0003-dbt-for-transforms.md))                            | Lineage, tests, docs free; adapter-neutral SQL.                                                         |
| **Typer CLI + cron over Airflow** ([ADR 0004](docs/decisions/0004-cron-over-airflow.md))                       | 8 API calls/day. Airflow is 100× the work.                                                              |
| **Single fact, nullable OHL** ([ADR 0005](docs/decisions/0005-single-fact-nullable-ohlc.md))                   | One join surface; `dim_asset.price_completeness` flags grain.                                           |

### Key trade-offs (not in ADRs)

- **`fact_daily_metrics` full-rebuild** — window functions over `date`; mid-history inserts invalidate downstream. < 1 s at 3k rows; incremental with `lookback_days` at >1M.
- **Calendar semantics per-metric** — per-asset metrics use each asset's own calendar; cross-asset metrics inner-join on `date`. Forward-filling BTC onto trading days would inflate correlation.
- **Rolling returns are calendar-day windows** — `rolling_return_Nd` uses `RANGE BETWEEN INTERVAL N DAY PRECEDING` ([macro](dbt/macros/rolling_return.sql)), not `LAG(close, N)`. 30-day AAPL = 30-day BTC in calendar duration. Analysis reads gold columns as-is — "compute once in gold, read many in analysis" is the medallion contract.
- **Silver MERGE is last-write-wins on `ingested_at`** — same-day replay is a no-op; corrected-price backfill wins.
- **Pandera `BronzeFrame` `strict=True`** — schema drift halts the pipeline; new field = 3-file edit. See [Schema evolution](#schema-evolution).
- **`dim_asset` is Type-1** — provider changes overwrite; SCD-2 is a one-model upgrade.
- **DuckDB single-writer lock** — CLI serializes ingest → dbt → analyze; no horizontal scale until ClickHouse swap.

USD-as-numéraire is the biggest trade-off — see [Core assumption](#core-assumption--usd-is-the-numéraire).

---

## Scaling path

| Layer                  | This demo                                               | At ~TB scale                                               |
| ---------------------- | ------------------------------------------------------- | ---------------------------------------------------------- |
| **Bronze**             | Local Parquet                                           | `BRONZE_URI=s3://…` + MinIO in CI                          |
| **Warehouse**          | DuckDB (embedded)                                       | ClickHouse — profile swap + MergeTree engine               |
| **Orchestrator**       | cron + Makefile                                         | Airflow — module entrypoints map 1:1 to tasks              |
| **Secrets**            | `.env` via pydantic-settings                            | AWS Secrets Manager / Vault                                |
| **Monitoring**         | structlog JSON + `meta.pipeline_runs` + healthchecks.io | Ship logs to Loki/Elasticsearch; alert on DQ test failures |
| **Ingest concurrency** | `asyncio.Semaphore` per provider                        | Same — limits are API-side, not infra-side                 |

S3 bronze works without code edits ([ADR 0002](docs/decisions/0002-bronze-parquet-fsspec-s3-ready.md)). **First bottleneck:** DuckDB's single-writer lock (→ ClickHouse). **Second:** `fact_daily_metrics` full-rebuild (→ incremental at >1M rows). Bronze + ingest scale cleanly (API-bound).

### 10-line Airflow DAG (migration stub)

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("assets_vs_btc", start_date=datetime(2026, 1, 1),
         schedule="0 2 * * *", catchup=False) as dag:
    ingest   = BashOperator(task_id="ingest",   bash_command="pipeline ingest")
    transform= BashOperator(task_id="transform",bash_command="pipeline transform")
    analyze  = BashOperator(task_id="analyze",  bash_command="pipeline analyze")
    ingest >> transform >> analyze
```

---

## Schema evolution

Fails closed on drift — silent schema changes are the worst wrong-answer source.

| Change upstream          | Triggers                            | Action                                       |
| ------------------------ | ----------------------------------- | -------------------------------------------- |
| New optional field       | Nothing (pydantic ignores unknowns) | None                                         |
| New required field       | Pydantic `ValidationError`          | Update [models.py](src/pipeline/models.py)   |
| New column in bronze     | Pandera rejects (`strict=True`)     | 3-file edit: pydantic → pandera → bronze     |
| New silver/gold column   | Manual                              | `ALTER TABLE ADD COLUMN` + dbt model         |
| Removed/renamed upstream | Pydantic fails                      | Update model; placeholder if silver needs it |

**Non-features:** no migration tool, no schema-version column, no producer-side contracts (pydantic is the implicit contract). See [Known gaps](#known-gaps).

---

## Performance

Full `make run`, 365-day window (8 symbols, ~3k rows), MacBook M-series:

| Stage        | Wall time    | Dominant cost                                         |
| ------------ | ------------ | ----------------------------------------------------- |
| Ingest       | 3–8 s        | API round-trips (8 calls, per-provider semaphore)     |
| Bronze write | < 100 ms     | Parquet encode + atomic `mv`                          |
| Silver MERGE | < 500 ms     | DuckDB `INSERT … ON CONFLICT`                         |
| dbt run      | 1–3 s        | 6 models; `fact_daily_metrics` full rebuild dominates |
| dbt test     | < 1 s        | 42 schema + 2 singular                                |
| Analysis     | < 500 ms     | Filter/aggregate gold + DCA/lump-sum sims             |
| HTML report  | < 1 s        | Jinja + inline plotly.js                              |
| **Total**    | **~10–15 s** | End-to-end                                            |

**Speedups if needed:** raise per-provider semaphore (halves ingest); read Parquet straight into DuckDB (~2×); `fact_daily_metrics` incremental with `lookback_days` (only worth it >1M rows). No perf regression test — see [Known gaps](#known-gaps).

---

## Scheduling

`make init` is the one-time setup command: bootstrap (schema DDL + `dim_date`
seed + seed fingerprints) **plus** `make run` (ingest → transform → analyze).
Everything after that is a `make run` — bootstrap is not recurring work:

```bash
make init             # one-time: bootstrap + run (use once per clone)
make run              # ingest → transform → analyze (daily; what cron runs)

make schedule         # install daily 02:00 UTC entry (calls `make run`)
make unschedule       # remove it
crontab -l            # verify
```

Both are idempotent — `schedule` replaces any prior entry pointing at this
repo; `unschedule` is a no-op if nothing is installed.

**Customizing the schedule**: override `CRON_SCHEDULE`, e.g.

```bash
make schedule CRON_SCHEDULE='*/5 * * * *'     # every 5 min (testing)
make schedule CRON_SCHEDULE='30 1 * * 1-5'    # weekdays 01:30 UTC
```

**How it works.** `make schedule` installs one crontab line pointing at
[scripts/cron-run.sh](scripts/cron-run.sh), a small wrapper that fixes the two
things cron's minimal environment doesn't do: it prepends Homebrew + user-local
bin dirs to `PATH` (so `uv` resolves) and redirects combined output to
`logs/cron-YYYYMMDD.log`. The wrapper invokes `make run` (**not** `make init`)
— scheduled fires do recurring data work only; schema DDL is a deliberate
deploy step, not a silent per-cron side effect. Every run stamps
`meta.pipeline_runs`; `make doctor` tables the last 5.

**Why 02:00 UTC**: CoinGecko publishes the completed UTC day at 00:35 UTC
(90 min settling buffer); US equity markets close by 21:00 UTC; FX settles by
22:00 UTC (NY close); DST-agnostic.

**Incremental-by-default**: with no `--start`/`--end`, `pipeline ingest` reads
`max(date)` per asset from silver and fetches from there to yesterday-UTC. A
multi-day outage self-heals on the next cron.

**Verify cron actually fires** (short-interval smoke test):

```bash
# 1. throwaway script that just timestamps a file
cat > /tmp/dpp-cron-test.sh <<'EOF'
#!/bin/bash
date -u +%Y-%m-%dT%H:%M:%SZ >> /tmp/dpp-cron-fires.log
EOF
chmod +x /tmp/dpp-cron-test.sh
rm -f /tmp/dpp-cron-fires.log

# 2. install every-minute via our target, overriding the script
make schedule CRON_SCHEDULE='* * * * *' CRON_SCRIPT=/tmp/dpp-cron-test.sh
crontab -l                                   # you should see the new line

# 3. wait ~2 min, then check fires
sleep 130 && cat /tmp/dpp-cron-fires.log     # expect ≥1 timestamp line

# 4. clean up
make unschedule CRON_SCRIPT=/tmp/dpp-cron-test.sh
rm /tmp/dpp-cron-test.sh /tmp/dpp-cron-fires.log
```

---

## Data quality & observability

### DQ gates — four layers, any one fails the pipeline

Non-zero exit propagates to cron `MAILTO` and healthchecks.io. Results
persist, so history is queryable without re-running.

| Gate                                           | Checks                                                                                                                                                                                                                                                                                                                     | Where                                                                                                       |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Pydantic v2**                                | API payload shape at HTTP boundary                                                                                                                                                                                                                                                                                         | [models.py](src/pipeline/models.py)                                                                         |
| **Pandera `BronzeFrame`** (`strict`, `coerce`) | `close` not-null; enum `source`/`asset_type`; OHLCV ≥ 0; type coercion                                                                                                                                                                                                                                                     | [quality/schemas.py](src/pipeline/quality/schemas.py) → [storage/bronze.py](src/pipeline/storage/bronze.py) |
| **Silver SQL assertions**                      | `unique_symbol_date`, `close_not_null`, `close_within_bounds`, per-symbol `freshness` (configurable lag)                                                                                                                                                                                                                   | [quality/assertions.py](src/pipeline/quality/assertions.py)                                                 |
| **dbt tests (gold)**                           | PK uniqueness + not-null on dims; FK `relationships` on both facts; OHLC ordering (`low ≤ {open,close} ≤ high`); `daily_return ∈ (−1, 1)`; `volume ≥ 0`; `base_ccy = 'USD'` (numéraire invariant); `source ∈ [massive, coingecko, synthetic]`; singular [no_calendar_gaps_stock.sql](dbt/tests/no_calendar_gaps_stock.sql) | [dbt/models/gold/schema.yml](dbt/models/gold/schema.yml)                                                    |

Silver results persist to `meta.fact_data_quality_runs` (`run_id, test_name, run_ts, passed, row_count, severity, details` — [run_tracker.py:104](src/pipeline/observability/run_tracker.py#L104)). Covers completeness, uniqueness, validity, consistency, timeliness; **accuracy** is the missing dimension ([Known gaps](#known-gaps)).

### Failure handling

dbt test fails → non-zero exit → [`_run_dbt`](src/pipeline/cli.py#L155) raises `SystemExit` → `track_run` flips `meta.pipeline_runs.status='failed'`, persists traceback, re-raises. Cron `MAILTO` mails stderr; healthchecks.io goes red; `make doctor` shows the failed run + offending test rows. Silver assertions: `severity='error'` raises `RuntimeError` in [`_do_ingest`](src/pipeline/cli.py#L35), short-circuiting before dbt.

### Test-layer ownership

- **dbt tests** — warehouse-table invariants (schemas, FKs, ranges, OHLC ordering).
- **pytest** — Python behaviour: `tests/unit/` (math/UTC/DCA/atomic writes), `tests/snapshot/` (golden outputs).
- **Silver SQL assertions** — fail-fast before dbt; configurable via `config/settings.yaml`.

Each check lives in exactly one layer.

### Observability

- **Exit codes** → cron `MAILTO` stderr.
- **Logs** — structlog JSON at `logs/pipeline-YYYY-MM-DD.log`; ship to Loki/Elasticsearch.
- **Run history** — `meta.pipeline_runs`: ULID `run_id`, timestamps, status, `rows_by_source`, `error_payload`, `git_sha`. Single `track_run` wraps ingest + dbt + analyze ([run_tracker.py:40](src/pipeline/observability/run_tracker.py#L40)); orphan `running` rows surface via `make doctor`.
- **DQ history** — `meta.fact_data_quality_runs`.
- **Healthchecks.io** (optional) — dead-man's switch.

`make doctor` = one-command health: runs, freshness, DQ failures, suggested backfills.

---

## Project layout

```
data-pipeline-poc/
├── README.md
├── ARCHITECTURE.html         # standalone C4 + sequence diagrams
├── Makefile                  # install | bootstrap | ingest | transform |
│                             # analyze | run | backfill | backfill-gaps |
│                             # doctor | test | lint | typecheck | ci
├── Dockerfile                # uv + pipeline runtime image
├── pyproject.toml            # uv-managed; ruff, mypy --strict, pytest
├── .env.example
├── .github/workflows/ci.yml  # ruff + mypy + pytest + dbt parse
├── config/settings.yaml      # assets, rate limits, DCA rule, analysis windows
├── src/
│   ├── pipeline/             # ingest + storage + quality + observability + CLI
│   │   ├── cli.py            # Typer entrypoint
│   │   ├── config.py         # pydantic-settings (env + YAML)
│   │   ├── models.py         # pydantic v2 API response schemas
│   │   ├── ingest/           # http, massive, coingecko, orchestrator
│   │   ├── storage/          # bronze (Parquet), warehouse (DuckDB MERGE)
│   │   ├── quality/          # pandera schemas + SQL assertions
│   │   └── observability/    # structlog + run_tracker + perf
│   └── analysis/             # returns, DCA, correlation, outputs, html_report
├── dbt/                      # profiles, models, tests, seeds, macros
├── tests/
│   ├── unit/                 # pure math + edge cases
│   ├── snapshot/             # golden-output regression
│   └── fixtures/             # golden CSVs + recorded API payloads (api_responses/)
├── docs/
│   ├── architecture.md       # C4 context + container (Mermaid)
│   ├── data_dictionary.md    # bronze/silver/gold column catalog
│   └── decisions/            # ADRs 0001–0005
├── scripts/                  # bootstrap_warehouse, seed_dim_date, record_fixtures, cron-run
├── data/                     # gitignored — bronze + warehouse.duckdb
├── logs/                     # gitignored — structlog JSON + cron stdout
├── outputs/                  # gitignored — regenerated CSV + Parquet (Q1–Q4 tables)
├── DATA_REPORTS/             # committed — data_analysis.md + data_analysis.html + static twin + performance_report.html
├── EXTRAS/                   # extra content (notes, screenshots)
```

---

## Automated tests

`make test` runs everything locally; CI
([.github/workflows/ci.yml](.github/workflows/ci.yml)) gates every push
on `ruff` → `mypy --strict` → `pytest` → `dbt parse`.

**Unit — [tests/unit/](tests/unit/)** (sub-second, pure functions):

| File                                                              | Covers                                                                                  |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| [test_returns.py](tests/unit/test_returns.py)                     | Window return math (1Y/YTD/6M/3M/1M/7D) + beat-BTC flagging                             |
| [test_dca.py](tests/unit/test_dca.py)                             | DCA vs lump-sum; "buy on 1st, else next trading day"                                    |
| [test_utc.py](tests/unit/test_utc.py)                             | CoinGecko ms → UTC `date`, DST-agnostic                                                 |
| [test_bronze_idempotent.py](tests/unit/test_bronze_idempotent.py) | Path determinism + overwrite-replay is a no-op                                          |
| [test_atomicity.py](tests/unit/test_atomicity.py)                 | Tmp-rename success/crash; run-tracker `running → success/failed` incl. `SIGKILL` orphan |
| [test_weekend.py](tests/unit/test_weekend.py)                     | Massive empty `results[]` on weekends = success                                         |
| [test_backfill.py](tests/unit/test_backfill.py)                   | `backfill-gaps` detects missing trading days                                            |
| [test_resolve_range.py](tests/unit/test_resolve_range.py)         | `--start`/`--end` override vs incremental `max(date)`                                   |

**Snapshot — [tests/snapshot/test_outputs.py](tests/snapshot/test_outputs.py)**: runs the analysis stack on a pinned synthetic dataset (400 days × 3 symbols, deterministic walk), asserts bit-equal hashes against golden CSVs, plus an HTML-render smoke test. Any formula change → hash shifts → red.

**dbt tests** — see [DQ gates](#dq-gates--four-layers-any-one-fails-the-pipeline). Run on every `make transform`, not just CI.

**API fixture replay — [tests/unit/test_ingest_fixtures.py](tests/unit/test_ingest_fixtures.py)**: drives `fetch_symbol` / `fetch_bitcoin` against `respx`-mocked Massive/CoinGecko endpoints loaded from `tests/fixtures/api_responses/*.json`. Catches payload-shape regressions (a renamed field, a type change) before they hit production. Refresh fixtures with [scripts/record_fixtures.py](scripts/record_fixtures.py) (needs a real `MASSIVE_API_KEY`); CI uses the committed fixtures so `MASSIVE_API_KEY=dummy-for-ci` is enough.

---

## Data dictionary

See [docs/data_dictionary.md](docs/data_dictionary.md) for the full
bronze/silver/gold column catalog.

## Data lineage

`make docs-dbt` runs `dbt docs generate` + `dbt docs serve` and opens an
interactive lineage graph at `http://localhost:8080` — every silver → gold
edge, per-model column descriptions, and attached tests in one place.
The underlying [dbt/target/manifest.json](dbt/target/manifest.json) +
[catalog.json](dbt/target/catalog.json) are static artifacts a CI job can
publish to a team-wide docs site.

---

## Risks & mitigations

- **CoinGecko 403 / rate-limit** → tenacity exponential backoff + jitter; persistent failure surfaces as a non-zero exit so cron / `make doctor` flags it instead of silently writing stale data.
- **Massive empty `results[]` on weekends** → treated as success ([test_weekend.py](tests/unit/test_weekend.py)).
- **DCA calendar ambiguity** → buy on 1st, else next trading day ([dca.py](src/analysis/dca.py)).
- **Timezone bugs** → UTC asserted at silver boundary ([test_utc.py](tests/unit/test_utc.py)).
- **Tiny-dataset-vs-patterns optics** → DuckDB + local Parquet for zero-friction local runs; star schema + fsspec bronze scale 5+ orders of magnitude.

---

## Known gaps

What a production deployment would want next.

| Area               | Gap                                                                                 | Remediation                                                                                                                                                 |
| ------------------ | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DQ — accuracy**  | No cross-source price reconciliation                                                | Diff today's BTC close vs. prior-day + rolling 7-day median; alert >50 bps unexplained drift                                                                |
| **DQ — anomalies** | Bounds check (`daily_return ∈ (−1, 1)`) misses sub-100% spikes                      | Singular dbt test: `abs(return) > 5σ` over rolling 30d                                                                                                      |
| **DQ — contract**  | No producer-side contract; pydantic is implicit                                     | Out of scope unless provider cooperates                                                                                                                     |
| **Schema**         | No migration tool — manual `ALTER TABLE`                                            | `alembic` or `dbt-ddl` macros                                                                                                                               |
| **Schema**         | `dim_asset` is Type-1 — updates overwrite, prior `source`/`price_completeness` lost | SCD-2: one row per asset _version_ with `valid_from`/`valid_to`; facts join on `date BETWEEN valid_from AND valid_to` so history keeps original attribution |
| **Testing**        | No integration test (ingest → silver → dbt end-to-end)                              | `tests/integration/` with `tmp_path` warehouse + fixtures                                                                                                   |
| **Testing**        | No perf regression test                                                             | Time `make run` in CI; fail if > N× baseline                                                                                                                |
| **Observability**  | No bronze-file lineage in silver                                                    | Add `bronze_path` column or `meta.fact_bronze_files`                                                                                                        |
| **Observability**  | No dashboard for `meta.*` tables                                                    | Grafana panel over DuckDB                                                                                                                                   |
| **Scale**          | Single-process ingest                                                               | Per-provider workers; clients already isolated                                                                                                              |
| **Performance**    | No query plan check on dbt models                                                   | `dbt compile` + `EXPLAIN ANALYZE` pre-merge                                                                                                                 |
