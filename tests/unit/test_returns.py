"""Unit tests for windowed returns and volatility aggregation.

After the gold-consumer migration, the analysis layer slices precomputed
metrics from `gold.fact_daily_metrics` instead of recomputing per-day math
from raw prices. Tests therefore feed a synthesized `metrics` DataFrame
(see `tests/conftest.py:_metrics_from_prices`) that mirrors gold.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from analysis.returns import returns_by_window, volatility_summary


def test_returns_by_window_flags_beats_btc(
    sample_prices: pl.DataFrame, sample_metrics: pl.DataFrame
) -> None:
    out = returns_by_window(
        sample_metrics,
        sample_prices,
        windows_days={"7d": 7, "1m": 30},
        as_of=date(2025, 2, 20),
        btc_symbol="BTC",
    )
    assert set(out["window"].unique().to_list()) >= {"7d", "1m", "ytd"}
    # beats_btc is boolean or None; BTC's own row is None
    btc_rows = out.filter(pl.col("symbol") == "BTC")
    assert btc_rows["beats_btc"].is_null().all()


def test_returns_by_window_falls_back_for_non_gold_window(
    sample_prices: pl.DataFrame, sample_metrics: pl.DataFrame
) -> None:
    """A window length not in {7,30,90,180,365} isn't precomputed in gold —
    `returns_by_window` should fall back to inline first/last close."""
    out = returns_by_window(
        sample_metrics,
        sample_prices,
        windows_days={"14d": 14},
        as_of=date(2025, 2, 20),
        btc_symbol="BTC",
    )
    rets = out.filter(pl.col("window") == "14d").drop_nulls("return")
    assert rets.height >= 1


def test_volatility_summary_per_symbol(sample_metrics: pl.DataFrame) -> None:
    vs = volatility_summary(sample_metrics, as_of=date(2025, 2, 20), lookback_days=60)
    assert set(vs["symbol"].to_list()) == {"BTC", "AAPL", "C:EURUSD"}
    assert (vs["daily_return_stdev"] >= 0).all()
