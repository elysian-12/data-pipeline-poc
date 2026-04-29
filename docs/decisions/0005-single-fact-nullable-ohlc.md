# ADR 0005 — Single `fact_daily_price` with nullable OHL

**Status**: Accepted
**Date**: 2026-04-19

## Context

We mix two source grains:

- **Massive** (stocks, FX, SPY): full OHLCV + VWAP + trade count.
- **CoinGecko** (BTC): daily close + volume only (no OHL on free tier).

The star schema needs one or more fact tables. The non-obvious call: do we
split the fact tables per grain, or unify them with nullable columns?

## Decision

**One `fact_daily_price` with nullable `open`, `high`, `low`.** The grain is
`(asset_id, date_id)`. A companion column on `dim_asset` —
`price_completeness ∈ {ohlcv, close_volume_only}` — tells downstream code
whether the OHL columns are meaningful.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Split facts: `fact_daily_ohlcv` + `fact_daily_close_volume`** | Correlation and volatility need `close` across **all** assets. Two facts force a `UNION ALL` in every cross-asset query — more joins, slower, easier to forget. The unified fact models the semantic truth ("daily observation of an asset") honestly. |
| **Synthesize missing OHL as `open=high=low=close`** | Lies to downstream. A technical analyst pulling `high - low` as a volatility proxy would get a silent zero for BTC. Explicit null is the honest signal. |
| **Fetch BTC OHLC from CoinGecko's `/ohlc` endpoint** | Free tier returns 4-day candles at ≥31 days; paid tier only for `interval=daily`. Doesn't solve the problem without a paid subscription. |

## Consequences

**Positive**

- One query shape for every cross-asset analysis.
- `price_completeness` surfaces the limitation in the star schema, not in
  scattered `if source == 'coingecko'` branches in Python.
- Easy to extend: add a `stablecoin` asset type later and the schema doesn't
  change.

**Negative**

- Queries that use OHL must filter `WHERE asset.price_completeness = 'ohlcv'`
  (or accept nulls). dbt tests enforce: `close NOT NULL` for all rows; OHL
  only required when `price_completeness = 'ohlcv'`.
- The volatility analysis uses `daily_return = close_t / close_{t-1} - 1`,
  not intraday (high − low) / close — consistent with what we can compute
  across all assets.

## Cost to reverse

**Low.** Splitting later is mechanical: `CREATE TABLE fact_daily_ohlcv AS
SELECT … WHERE price_completeness = 'ohlcv'` + update consumers. But the
honest grain is the unified one, and we'd probably never reverse this.
