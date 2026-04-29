"""Pearson correlation matrix on `gold.fact_daily_metrics.daily_return`.

The correlation matrix shape (symbol × symbol) is presentation-layer and
intentionally not precomputed in gold. The `daily_return` series itself is
already in gold — this module pivots and correlates rather than recomputing
the per-day return.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl


def correlation_matrix(
    metrics: pl.DataFrame, *, as_of: date, lookback_days: int = 365
) -> pl.DataFrame:
    """Wide Pearson correlation matrix across symbols' daily returns.

    Windowed to `[as_of - lookback_days, as_of]` to match the rest of the
    analysis layer — otherwise a multi-year warehouse drifts the matrix away
    from the 1-year story the report tells.

    Inner-joined on date — only dates where every symbol has data are used.
    """
    start = as_of - timedelta(days=lookback_days)
    # Strict `date > start` mirrors the prior polars path: that path filtered
    # `date >= start` then dropped null daily_return rows — including the
    # boundary row whose shift(1) was null. Excluding the boundary explicitly
    # here keeps Pearson values bit-identical to the pre-migration baseline.
    windowed = metrics.filter((pl.col("date") > start) & (pl.col("date") <= as_of)).drop_nulls(
        "daily_return"
    )

    # A symbol with zero daily-return variance (e.g. the synthetic USD numéraire
    # at close=1.0) cannot correlate with anything — Pearson divides by stdev.
    # Dropping it keeps the matrix finite and meaningful.
    non_flat = (
        windowed.group_by("symbol")
        .agg(pl.col("daily_return").std().alias("stdev"))
        .filter(pl.col("stdev") > 0)
        .select("symbol")
    )
    windowed = windowed.join(non_flat, on="symbol", how="inner")

    wide = windowed.pivot(
        values="daily_return", index="date", on="symbol", aggregate_function="first"
    ).drop_nulls()  # inner-join by dropping any date with a missing column

    symbols = [c for c in wide.columns if c != "date"]
    corr_values = wide.select(symbols).corr().to_numpy()

    return pl.DataFrame(
        {"symbol": symbols} | {sym: corr_values[:, i].tolist() for i, sym in enumerate(symbols)}
    )
