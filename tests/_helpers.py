"""Test-only helpers shared by unit and snapshot suites."""

from __future__ import annotations

from datetime import timedelta

import polars as pl


def metrics_from_prices(prices: pl.DataFrame) -> pl.DataFrame:
    """Polars analogue of `dbt/models/gold/fact_daily_metrics.sql`.

    Lets analysis-layer tests feed `returns_by_window`, `volatility_summary`,
    `correlation_matrix`, and `_q4_rolling_chart` without standing up a real
    DuckDB warehouse. Mirrors gold's formulas exactly:

    - daily_return = close / lag(close) - 1
    - log_return   = ln(close / lag(close))
    - rolling_return_Nd = close / first close in `[date - N days, date]` - 1
      (calendar-day window — matches the dbt macro after M1).
    - rolling_vol_30d   = stddev(daily_return) over the trailing 30 daily
      observations (sample stdev, n-1, matching DuckDB STDDEV).
    """
    df = prices.sort(["symbol", "date"]).with_columns(
        daily_return=(pl.col("close") / pl.col("close").shift(1).over("symbol") - 1.0),
        log_return=(pl.col("close") / pl.col("close").shift(1).over("symbol")).log(),
    )
    pieces: list[pl.DataFrame] = []
    for sym in df["symbol"].unique().to_list():
        s = df.filter(pl.col("symbol") == sym).sort("date")
        dates = s["date"].to_list()
        closes = s["close"].to_list()
        for n in (7, 30, 90, 180, 365):
            firsts: list[float | None] = []
            for i, d in enumerate(dates):
                lower = d - timedelta(days=n)
                anchor_close = None
                for j in range(i + 1):
                    if dates[j] >= lower:
                        anchor_close = closes[j]
                        break
                if anchor_close is None or anchor_close == 0:
                    firsts.append(None)
                else:
                    firsts.append(closes[i] / anchor_close - 1.0)
            s = s.with_columns(pl.Series(f"rolling_return_{n}d", firsts))
        pieces.append(s)
    df = pl.concat(pieces, how="vertical")
    df = df.with_columns(
        rolling_vol_30d=pl.col("daily_return").rolling_std(window_size=30).over("symbol"),
    )
    df = df.with_columns(rel_perf_vs_btc=pl.lit(None, dtype=pl.Float64))
    return df.sort(["symbol", "date"])
