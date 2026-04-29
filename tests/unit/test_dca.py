"""DCA & lump-sum: calendar edge cases + math."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from analysis.dca import compare_dca_vs_lump_sum, dca, lump_sum


def test_lump_sum_reports_positive_return_on_rising_series() -> None:
    df = pl.DataFrame(
        {
            "symbol": ["BTC"] * 4,
            "date": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 1), date(2025, 1, 1)],
            "close": [40_000.0, 60_000.0, 90_000.0, 100_000.0],
        }
    )
    r = lump_sum(
        df, symbol="BTC", principal_usd=1000.0, start=date(2024, 1, 1), end=date(2025, 1, 1)
    )
    assert r is not None
    assert r.total_return > 1.0  # > 100%
    assert abs(r.current_value_usd - 2500.0) < 1e-6


def test_dca_falls_forward_on_missing_day() -> None:
    # No close on the 1st — DCA should fall forward to the 3rd.
    df = pl.DataFrame(
        {
            "symbol": ["BTC"] * 3,
            "date": [date(2024, 1, 3), date(2024, 2, 3), date(2024, 3, 3)],
            "close": [40_000.0, 50_000.0, 60_000.0],
        }
    )
    r = dca(
        df,
        symbol="BTC",
        monthly_amount_usd=100.0,
        months=3,
        start=date(2024, 1, 1),
        as_of=date(2024, 3, 3),
        buy_day_of_month=1,
    )
    assert r is not None
    assert len(r.buys) == 3
    assert r.buys[0][0] == date(2024, 1, 3)
    assert abs(r.total_invested_usd - 300.0) < 1e-6


def test_compare_dca_vs_lump_sum_both_strategies_reported() -> None:
    rows: list[dict[str, object]] = []
    start = date(2024, 4, 1)
    price = 40_000.0
    for i in range(400):
        d = start + timedelta(days=i)
        price *= 1.001
        rows.append({"symbol": "BTC", "date": d, "close": price})
    df = pl.DataFrame(rows)
    out = compare_dca_vs_lump_sum(
        df,
        symbol="BTC",
        monthly_amount_usd=100.0,
        months=12,
        buy_day_of_month=1,
        as_of=date(2025, 4, 1),
    )
    strategies = set(out["strategy"].to_list())
    assert strategies == {"dca", "lump_sum"}
