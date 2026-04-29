"""Return & volatility aggregations.

Thin consumer of `gold.fact_daily_metrics` — daily / log / rolling-N-day
returns are precomputed in dbt; this module slices and aggregates rather
than recomputing.

Two functions:
- `returns_by_window`: pick the rolling-return-N column for each named
  window (YTD is variable-length and still computed inline).
- `volatility_summary`: stdev of `daily_return` over the lookback window.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

# Map of canonical short-window labels (e.g. "1m") to the corresponding
# `gold.fact_daily_metrics` column. Keep keys in sync with the dbt macro
# invocations in `dbt/models/gold/fact_daily_metrics.sql`.
_GOLD_ROLLING_RETURN_COLS: dict[int, str] = {
    7: "rolling_return_7d",
    30: "rolling_return_30d",
    90: "rolling_return_90d",
    180: "rolling_return_180d",
    365: "rolling_return_365d",
}


def _last_value_at_or_before(
    df: pl.DataFrame, *, symbol: str, column: str, as_of: date
) -> float | None:
    """Latest non-null `column` for `symbol` on or before `as_of`."""
    s = df.filter((pl.col("symbol") == symbol) & (pl.col("date") <= as_of)).drop_nulls(column)
    if s.is_empty():
        return None
    return float(s.sort("date").tail(1).select(column).item())


def _ytd_return(prices: pl.DataFrame, *, symbol: str, year: int, as_of: date) -> float | None:
    """First/last close from the YTD window — variable-length, can't come from gold."""
    start = date(year, 1, 1)
    s = prices.filter(
        (pl.col("symbol") == symbol) & (pl.col("date") >= start) & (pl.col("date") <= as_of)
    ).sort("date")
    if s.is_empty():
        return None
    first = float(s.head(1).select("close").item())
    last = float(s.tail(1).select("close").item())
    return (last / first) - 1.0


def returns_by_window(
    metrics: pl.DataFrame,
    prices: pl.DataFrame,
    *,
    windows_days: dict[str, int],
    as_of: date,
    btc_symbol: str,
) -> pl.DataFrame:
    """One row per (symbol, window) with return vs BTC's return in the same window.

    `metrics` is `gold.fact_daily_metrics` — provides `rolling_return_{7,30,90,180,365}d`
    over a calendar-day window (`RANGE INTERVAL N DAY PRECEDING`). `prices` is
    only used for the YTD branch, where window length depends on `as_of` and
    can't be precomputed in gold.
    """
    rows: list[dict[str, object]] = []
    symbols = prices["symbol"].unique().to_list()

    def _value_for(sym: str, days: int) -> float | None:
        col = _GOLD_ROLLING_RETURN_COLS.get(days)
        if col is None:
            # Fallback for non-standard windows: first/last close inline.
            start = as_of - timedelta(days=days)
            s = prices.filter(
                (pl.col("symbol") == sym) & (pl.col("date") >= start) & (pl.col("date") <= as_of)
            ).sort("date")
            if s.is_empty():
                return None
            first = float(s.head(1).select("close").item())
            last = float(s.tail(1).select("close").item())
            return (last / first) - 1.0
        return _last_value_at_or_before(metrics, symbol=sym, column=col, as_of=as_of)

    # Fixed-length windows from gold.
    for label, days in windows_days.items():
        btc_ret = _value_for(btc_symbol, days)
        w_start = as_of - timedelta(days=days)
        for sym in symbols:
            ret = _value_for(sym, days)
            rows.append(
                {
                    "window": label,
                    "symbol": sym,
                    "start_date": w_start,
                    "end_date": as_of,
                    "return": ret,
                    "btc_return": btc_ret,
                    "beats_btc": (
                        None
                        if ret is None or btc_ret is None or sym == btc_symbol
                        else ret > btc_ret
                    ),
                }
            )

    # YTD — variable-length, computed inline from prices.
    ytd_start = date(as_of.year, 1, 1)
    btc_ytd = _ytd_return(prices, symbol=btc_symbol, year=as_of.year, as_of=as_of)
    for sym in symbols:
        ret = _ytd_return(prices, symbol=sym, year=as_of.year, as_of=as_of)
        rows.append(
            {
                "window": "ytd",
                "symbol": sym,
                "start_date": ytd_start,
                "end_date": as_of,
                "return": ret,
                "btc_return": btc_ytd,
                "beats_btc": (
                    None if ret is None or btc_ytd is None or sym == btc_symbol else ret > btc_ytd
                ),
            }
        )
    return pl.DataFrame(rows)


def volatility_summary(metrics: pl.DataFrame, *, as_of: date, lookback_days: int) -> pl.DataFrame:
    """Per-asset stdev of `daily_return` over the lookback window. Answers Q4.

    Uses gold's precomputed `daily_return`. Window is open on the left
    (``date > start``) to match the prior polars path: that path filtered
    ``date >= start`` and then dropped the first daily_return per symbol
    (which was null because shift(1) at the slice boundary produced NaN).
    Strict ``> start`` is the cleanest expression of the same observation
    set, and keeps Bessel-corrected sample stdev bit-identical to the
    pre-migration baseline.
    """
    start = as_of - timedelta(days=lookback_days)
    slc = metrics.filter((pl.col("date") > start) & (pl.col("date") <= as_of)).drop_nulls(
        "daily_return"
    )
    return (
        slc.group_by(["symbol", "asset_type"])
        .agg(
            pl.col("daily_return").std().alias("daily_return_stdev"),
            pl.col("daily_return").count().alias("n_obs"),
        )
        .sort("symbol")
    )
