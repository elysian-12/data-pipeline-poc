# Testing

## Philosophy

The suite favors **behavioral** tests over implementation tests: we pin the
observable contract (idempotent MERGE, atomic bronze writes, calendar math)
rather than internal function shapes. Three layers:

| Layer | Location | Purpose |
|---|---|---|
| **Unit** | `tests/unit/` | Pure-function correctness and storage invariants — each test isolates one boundary. |
| **Snapshot** | `tests/snapshot/` | End-to-end stability of analysis outputs on a pinned synthetic dataset. Catches silent regressions from library upgrades or formula drift. |
| **Live** | `make run` against real Massive / CoinGecko | Not in `pytest`; verified manually + via CI smoke-run. |

**Run:** `uv run pytest -q` (38 tests, ≈3s on DuckDB in-memory).

Shared fixtures in [tests/conftest.py](../tests/conftest.py):

- `sample_prices` — deterministic zig-zag close-price series for 3 symbols × 60 days. Schema: `{symbol, asset_type, date, close}`.
- `sample_metrics` — polars analogue of `gold.fact_daily_metrics` synthesised from `sample_prices`. Used by analysis-layer tests so they don't need to stand up a real DuckDB warehouse to feed the gold-consuming functions.
- `tmp_settings` — isolated DuckDB path + bronze URI via monkeypatched env.
- `warehouse_conn` — bootstrapped DuckDB handle.
- `silver_seeder` / `make_bronze_row` — MERGE helpers for synthetic silver data.

The metrics-from-prices helper lives in [tests/_helpers.py](../tests/_helpers.py) — same formulas as the dbt model (`daily_return`, `log_return`, `rolling_return_{7,30,90,180,365}d`, `rolling_vol_30d`) so analysis-layer behaviour matches end-to-end pipeline behaviour bit-for-bit.

---

## Unit tests

### [test_utc.py](../tests/unit/test_utc.py) — ms-epoch → UTC date

**Guards against:** silent DST/local-time off-by-one bucketing of daily bars.
**Guarantees:**
- Millisecond epoch at 07:00 UTC maps to the UTC calendar date, not US-local.
- Midnight UTC doesn't roll back a day.
- Pydantic alias `l → low` is populated.

### [test_weekend.py](../tests/unit/test_weekend.py) — empty-results is a success

**Guards against:** treating legitimate empty weekend/holiday API responses as errors — which would poison the run tracker and stall backfills.
**Guarantees:**
- `fetch_symbol` with `results=[]` returns `[]`, not an exception.
- A single trading-day payload parses into one typed `BronzeRow` with correct `close` + `date`.

### [test_backfill.py](../tests/unit/test_backfill.py) — silver MERGE idempotency

**Guards against:** duplicate rows on retry, stale batches overwriting fresh data.
**Guarantees:**
- MERGE on `(symbol, date)` is idempotent — re-runs are no-ops.
- `ingested_at` is the tiebreaker: later wins, stale is ignored.
- Manual deletes + re-ingest with later `ingested_at` is a supported gap-fill path.

### [test_resolve_range.py](../tests/unit/test_resolve_range.py) — range precedence

**Guards against:** refetching everything (quota burn) or silently skipping days.
**Guarantees:**
- Explicit `--start`/`--end` args always win.
- Incremental mode: `start = min(latest_per_symbol) + 1 day`, `end = yesterday UTC`.
- Cold start: `start = today - default_lookback_days`, `end = yesterday UTC`.

### [test_bronze_idempotent.py](../tests/unit/test_bronze_idempotent.py) — bronze path + overwrite

**Guards against:** ghost partitions from nondeterministic paths and row duplication on re-run.
**Guarantees:**
- `bronze_path()` is a pure function of `(source, asset_type, ingested_date)`.
- Path contains the expected Hive-style segments.
- `write_bronze` overwrites on re-run — row count matches the latest call.

### [test_atomicity.py](../tests/unit/test_atomicity.py) — tmp-file rename + run tracker lifecycle

**Guards against:** partial parquet files corrupting the lake; crashed runs leaving no audit row.
**Guarantees:**
- Successful bronze writes leave no `.tmp.` debris; final file is complete.
- A mid-write crash leaves neither final file nor tmp debris.
- `track_run` writes exactly one row: `status='running'` during the block, `success` + `ended_at` on clean exit, `failed` + stack trace on exception.

### [test_returns.py](../tests/unit/test_returns.py) — return math (gold-consuming)

**Guards against:** wrong denominators, missing-symbol fallback regressions, BTC self-comparison bugs — all silent killers of analysis output.
**Guarantees:**
- `returns_by_window` reads `gold.fact_daily_metrics.rolling_return_Nd` for the canonical {7, 30, 90, 180, 365}-day windows.
- Non-canonical windows (e.g. 14d) fall back to inline first/last-close arithmetic from `prices`.
- `beats_btc` is null for BTC's own rows; the windows set includes `ytd` (variable-length, computed inline).
- `volatility_summary` aggregates gold's `daily_return` over the lookback window; per-symbol stdev is non-negative and non-null.

### [test_dca.py](../tests/unit/test_dca.py) — DCA + lump-sum math

**Guards against:** the classic DCA bugs — missing month-start buys, weekend edges, off-by-one unit math.
**Guarantees:**
- Lump-sum on a known rising series produces the exact PnL + return.
- DCA falls forward when the target buy day is absent; `total_invested = months × monthly_amount`.
- `compare_dca_vs_lump_sum` returns both strategy rows.

### [test_perf.py](../tests/unit/test_perf.py) — perf log + report rendering

**Guards against:** broken stage-timing JSONL writes; perf-report HTML drift on memory-tracking schema changes; old-format JSONL crashing the renderer after a memory-tracking upgrade.
**Guarantees:**
- `timed()` writes one JSONL line per stage with `duration_s`, `status`, and `rss_mb_{start,peak,end}` — peak ≥ start, peak ≥ end.
- Stage exceptions still flush a record with `status='error'`.
- `clear_perf_log()` deletes the log idempotently.
- `write_perf_report()` renders peak-RSS + Δ-RSS columns and a "Peak RSS" KPI cell with formatted MB values.
- Records missing `rss_mb_*` fields fall back to em-dash placeholders instead of crashing (back-compat with pre-memory JSONL).
- Empty log produces a "No performance records" page rather than an exception.

---

## Snapshot tests

### [test_outputs.py](../tests/snapshot/test_outputs.py) — end-to-end stability

A pinned 400-day synthetic dataset flows through the full analysis pipeline; outputs are hashed or compared within tight tolerances.

**Guards against:** polars/duckdb version drift, formula regressions slipping past unit tests, and a silently broken HTML report (missing sections, no Plotly).
**Guarantees:**
- `returns_by_window` CSV matches a golden hash.
- `volatility_summary` stdevs match within `1e-9`.
- DCA-vs-lump PnL columns match within `1e-6`.
- Correlation matrix shape is `(3, 4)` with diagonal `= 1.0`.
- Rendered HTML is >500KB, contains Q1–Q4 + Correlation headings, a valid `<!doctype html>`, and `class="story"` in ≥4 places (confirms Plotly was inlined — not a stub).

---

## What's explicitly not tested

- **Live API correctness** — Massive / CoinGecko payloads are replayed in [tests/unit/test_ingest_fixtures.py](../tests/unit/test_ingest_fixtures.py) via `respx` against snapshots in [tests/fixtures/api_responses/](../tests/fixtures/api_responses/). Catches payload-shape regressions (renamed fields, type changes) but not live-network behavior — refresh fixtures with `uv run python scripts/record_fixtures.py` (needs a real `MASSIVE_API_KEY`); live-smoke is a manual `make run` step.
- **dbt model semantics** — covered by `dbt test` (unique/not_null/relationships + a singular calendar-gap test), not pytest.
- **Plotly chart rendering** — we assert structural markers (headings, script tags, byte size) but not pixel output.
