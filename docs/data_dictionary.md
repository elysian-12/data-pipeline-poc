# Data dictionary

Column-level catalog for every table and file produced by the pipeline.
Organized by medallion layer, then by table.

---

## Bronze — partitioned Parquet

**Path**: `{BRONZE_URI}/source={source}/asset_type={asset_type}/ingested_date={YYYY-MM-DD}/data.parquet`

**Partition columns** (expressed as Hive-style path keys, not stored in the
file body):

| Partition key | Values | Meaning |
|---|---|---|
| `source` | `massive`, `coingecko`, `synthetic` | Origin of the row; enables per-source replay / debug. |
| `asset_type` | `stock`, `fx`, `index`, `crypto` | Matches `dim_asset_type.name`. |
| `ingested_date` | `YYYY-MM-DD` (UTC) | The date the pipeline ran, **not** the price date. Deterministic path — re-runs overwrite. |

**Row schema** (pydantic `BronzeRow`, serialized to Parquet):

| Column | Type | Null? | Description |
|---|---|---|---|
| `source` | string | no | Echoes the partition — denormalized for tools that read files without partition context. |
| `asset_type` | string | no | Echoes the partition. |
| `symbol` | string | no | `AAPL`, `C:EURUSD`, `BTC`, `USD`, etc. Matches `dim_asset.symbol`. |
| `date` | date | no | The **trading date** of the row (UTC). |
| `open` | double | yes | Nullable — close-volume-only sources (CoinGecko BTC) leave OHL blank. |
| `high` | double | yes | As above. |
| `low` | double | yes | As above. |
| `close` | double | no | Always present. Primary signal. |
| `volume` | double | yes | Reported volume. Null for synthetic USD. |
| `vwap` | double | yes | Volume-weighted avg price (Massive only). |
| `trade_count` | int | yes | Trade count for the day (Massive only). |
| `ingested_at` | timestamp (UTC) | no | **When this row was fetched** — used by silver MERGE for last-write-wins. |
| `run_id` | string | no | ULID of the pipeline invocation that wrote this row. Joins to `meta.pipeline_runs.run_id`. |

---

## Silver — DuckDB

### `silver.stg_prices`

One row per `(symbol, date)`. This is the immutable-by-MERGE staging area
from which dbt builds gold.

| Column | Type | Null? | Description |
|---|---|---|---|
| `source` | VARCHAR | no | `massive` / `coingecko` / `synthetic`. |
| `asset_type` | VARCHAR | no | Redundant with `dim_asset_type`; carried for debug. |
| `symbol` | VARCHAR | no | PK component. |
| `date` | DATE | no | PK component. |
| `open`, `high`, `low` | DOUBLE | yes | Nullable for close-volume-only sources. |
| `close` | DOUBLE | no | |
| `volume`, `vwap` | DOUBLE | yes | |
| `trade_count` | INTEGER | yes | |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | no | MERGE key for last-write-wins. |
| `run_id` | VARCHAR | no | Lineage pointer to `meta.pipeline_runs`. |

**Primary key**: `(symbol, date)`.

**MERGE semantics**:

```sql
INSERT INTO silver.stg_prices VALUES (...)
ON CONFLICT (symbol, date) DO UPDATE SET
    open = EXCLUDED.open,
    ...
    ingested_at = EXCLUDED.ingested_at,
    run_id = EXCLUDED.run_id
WHERE EXCLUDED.ingested_at > silver.stg_prices.ingested_at;
```

Older `ingested_at` batches are silently dropped — replayed / out-of-order
data cannot corrupt silver.

---

## Gold — dbt models (star schema)

### `gold.dim_asset_type`

| Column | Type | Description |
|---|---|---|
| `asset_type_id` | INTEGER | Surrogate key. |
| `name` | VARCHAR | `stock`, `fx`, `index`, `crypto`. |

### `gold.dim_asset`

| Column | Type | Description |
|---|---|---|
| `asset_id` | INTEGER | Surrogate key — stable per `symbol`. |
| `symbol` | VARCHAR | Natural key. |
| `name` | VARCHAR | Human-readable name (from seed). |
| `asset_type_id` | INTEGER | FK → `dim_asset_type`. |
| `source` | VARCHAR | Primary source for this asset — `massive`, `coingecko`, `synthetic`. |
| `price_completeness` | VARCHAR | `ohlcv` (Massive) or `close_volume_only` (CoinGecko) — tells downstream whether OHL are meaningful. |
| `base_ccy` | VARCHAR | `USD` for all rows in this demo. |

### `gold.dim_date`

| Column | Type | Description |
|---|---|---|
| `date_id` | INTEGER | Surrogate key (`YYYYMMDD` as int). |
| `date` | DATE | The actual date. |
| `year`, `quarter`, `month`, `day`, `dow` | INTEGER | Calendar parts. |
| `is_trading_day` | BOOLEAN | NYSE calendar (from `pandas_market_calendars`). Used by gap-detection and DQ tests. |

### `gold.fact_daily_price`

**Grain**: one row per `(asset_id, date_id)`.

| Column | Type | Null? | Description |
|---|---|---|---|
| `asset_id` | INTEGER | no | FK → `dim_asset`. Part of PK. |
| `date_id` | INTEGER | no | FK → `dim_date`. Part of PK. |
| `open`, `high`, `low` | DOUBLE | yes | Nullable for close-volume-only sources. |
| `close` | DOUBLE | no | |
| `volume`, `vwap` | DOUBLE | yes | |
| `trade_count` | INTEGER | yes | Massive only; null elsewhere. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | no | Carried from silver — used by the incremental watermark (`ingested_at > MAX(this.ingested_at)`). |

**Materialization**: `incremental, unique_key=['asset_id','date_id'], on_schema_change='append_new_columns'`.
Backfill-safe — new rows merge on PK; existing rows are updated from silver.

`source` and `run_id` deliberately stay in silver, not gold — gold is the modelled, query-friendly surface; lineage to bronze is recovered by joining silver on `(symbol, date)` when needed.

### `gold.fact_daily_metrics`

**Grain**: one row per `(asset_id, date_id)`. Computed from `fact_daily_price`.

| Column | Type | Description |
|---|---|---|
| `asset_id` | INTEGER | FK → `dim_asset`. |
| `date_id` | INTEGER | FK → `dim_date`. |
| `daily_return` | DOUBLE | `close_t / close_{t-1} - 1` — per asset's own calendar. |
| `log_return` | DOUBLE | `ln(close_t / close_{t-1})`. |
| `rolling_return_7d` | DOUBLE | Close-to-close return over the trailing 7 **calendar** days (`RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW`) — not 7 trading rows. Cross-asset comparable: a 30d AAPL return covers the same calendar span as a 30d BTC return even though BTC has 7-day weeks. |
| `rolling_return_30d` | DOUBLE | …30 calendar days. |
| `rolling_return_90d` | DOUBLE | …90 calendar days. |
| `rolling_return_180d` | DOUBLE | …180 calendar days. |
| `rolling_return_365d` | DOUBLE | …365 calendar days. |
| `rolling_vol_30d` | DOUBLE | Sample stdev (n−1) of `daily_return` over the trailing 30 **rows** (`ROWS BETWEEN 29 PRECEDING AND CURRENT ROW`), gated on `COUNT(daily_return) >= 30` so partial windows return NULL. Row-window — not calendar-window — because daily-return values don't exist on non-trading days for stocks/FX, so 30 rows = 30 observations. The first 30 rows per asset are null (row 1's `daily_return` is itself NULL, so 30 non-null observations are accumulated by row 31). |
| `rel_perf_vs_btc` | DOUBLE | `rolling_return_30d[asset] - rolling_return_30d[BTC]`, BTC inner-joined on `date`. |

**Materialization**: `table` (full rebuild each run). Reason: rolling windows
and vol are window functions over `date`. Inserting a row in the middle of
history invalidates every downstream value. Incremental materialization would
be wrong. At ~3k rows, full rebuild is sub-second. See
[ADR 0005](decisions/0005-single-fact-nullable-ohlc.md) for the related
grain decision.

---

## Meta — pipeline observability

### `meta.pipeline_runs`

Append-only; one row per `make run` / `pipeline ingest` invocation.

| Column | Type | Description |
|---|---|---|
| `run_id` | VARCHAR | ULID — monotonic, timestamp-prefixed. |
| `started_at` | TIMESTAMP WITH TIME ZONE | |
| `ended_at` | TIMESTAMP WITH TIME ZONE | Null while in-flight. |
| `status` | VARCHAR | `running`, `success`, `failed`. |
| `rows_by_source` | JSON | `{"massive": 1760, "coingecko": 365, "synthetic": 365}` — observability without joining bronze. |
| `error_payload` | JSON | Populated on failure: `{type, message, traceback}`. |
| `git_sha` | VARCHAR | From `$GIT_SHA` env; blank locally. |

### `meta.fact_data_quality_runs`

One row per assertion per run — DQ history for trend analysis.

| Column | Type | Description |
|---|---|---|
| `run_id` | VARCHAR | FK → `meta.pipeline_runs.run_id`. |
| `test_name` | VARCHAR | e.g. `unique_symbol_date`, `close_not_null`, `freshness`. |
| `run_ts` | TIMESTAMP WITH TIME ZONE | |
| `passed` | BOOLEAN | |
| `row_count` | INTEGER | Rows examined. |
| `severity` | VARCHAR | `warn` or `error`. `error` fails the pipeline; `warn` logs and continues. |
| `details` | JSON | Free-form context — e.g. the violating rows or the threshold value. |

---

## Outputs — `outputs/*.csv` + `*.parquet`

### `returns_by_window` (answers Q1)

One row per `(window, symbol)`.

| Column | Type | Description |
|---|---|---|
| `window` | string | `7d`, `1m`, `3m`, `6m`, `1y`, `ytd`. |
| `symbol` | string | Asset symbol. |
| `start_date` | date | Window start. |
| `end_date` | date | Window end (`as_of`). |
| `return` | float | `(end_close / start_close) - 1`; null if either anchor is missing. |
| `btc_return` | float | BTC's return over the same window — denormalized for direct comparison. |
| `beats_btc` | bool | `return > btc_return`; null for the BTC row itself or when either return is null. |

### `lump_sum_1k` (answers Q2)

One row per `symbol`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | |
| `principal_usd` | float | Initial investment (1000.0 for the standard run). |
| `units` | float | `principal_usd / start_price`. |
| `start_date` | date | First available trading day on/after the pinned window start (1Y ago from `as_of`). |
| `end_date` | date | Latest available trading day on/before `as_of`. |
| `start_price` | float | Close on `start_date`. |
| `end_price` | float | Close on `end_date`. |
| `current_value_usd` | float | `units * end_price`. |
| `pnl_usd` | float | `current_value_usd - principal_usd`. |
| `total_return` | float | `pnl_usd / principal_usd`. |

### `dca_vs_lump` (answers Q3)

Two rows: `strategy ∈ {dca, lump_sum}`, both for BTC.

| Column | Type | Description |
|---|---|---|
| `strategy` | string | `dca` or `lump_sum`. |
| `symbol` | string | `BTC`. |
| `principal_usd` | float | $1,200 for both — equal-principal head-to-head. |
| `n_buys` | int | 12 for DCA (one per month); 1 for lump sum. |
| `units` | float | Total accumulated units. |
| `current_value_usd` | float | |
| `pnl_usd` | float | |
| `total_return` | float | |

**DCA rule**: buy on the 1st of each month; if the market is closed, roll
forward to the next trading day. Encoded in
[`_price_on_or_after`](../src/analysis/dca.py).

### `volatility_summary` (answers Q4)

One row per symbol; fiat / BTC grouping via `asset_type`.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | |
| `asset_type` | string | `stock`, `fx`, `index`, `crypto`. |
| `daily_return_stdev` | float | σ(`daily_return`) over a 365-day lookback ending at `as_of`. |
| `n_obs` | int | Count of non-null daily returns inside the lookback — exposes the asymmetry between crypto (≈365 obs) and stocks/FX (≈252 obs) so consumers can annualize correctly. |

### `correlation_matrix`

Square Pearson correlation on inner-joined dates.

| Column | Type | Description |
|---|---|---|
| `symbol` | string | Row label. |
| `<symbol>` | float | One column per symbol; the diagonal is `~1.0`. |

---

## Narrative reports — `DATA_REPORTS/`

### `data_analysis.md`

Human-readable markdown. Sections:

- **Summary** — latest run per source, total rows, DQ pass/fail.
- **Q1 — Outperformers vs BTC** — markdown summary from `returns_by_window`.
- **Q2 — $1,000 invested 1Y ago** — markdown summary from `lump_sum_1k`.
- **Q3 — DCA vs lump sum (BTC)** — markdown summary from `dca_vs_lump`.
- **Q4 — Fiat vs BTC volatility** — markdown summary from `volatility_summary`.
- **Warnings** — any DQ `warn` rows from the current run.

### `data_analysis.html` and `data_analysis_static.html`

Two variants of the visual panel — same content, different bundling:

- `data_analysis.html` — plotly.js loaded from a CDN; ~700 KB; renders on
  GitHub's web UI; requires internet.
- `data_analysis_static.html` — plotly.js embedded; ~5 MB; self-contained,
  works without network.

Both have one `<section>` per question, each with its best-fit chart and a
short data-storytelling paragraph:

- **Q1** — cumulative price lines rebased to 100 over 1Y + ranked bars per window.
- **Q2** — horizontal ranked bar of current USD value per asset.
- **Q3** — portfolio-value time series: DCA (step-up) vs lump sum (all-in day 1).
- **Q4** — rolling 30-day annualised volatility line chart per symbol.
- **Correlation** — Pearson heatmap with diverging colour scale.
