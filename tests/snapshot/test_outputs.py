"""Snapshot: analysis outputs on a pinned synthetic dataset are stable across runs."""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from analysis.correlation import correlation_matrix
from analysis.dca import compare_dca_vs_lump_sum, lump_sum
from analysis.html_report import write_html_report
from analysis.returns import returns_by_window, volatility_summary
from pipeline.config import AnalysisConfig, DCAConfig, LumpSumConfig
from tests._helpers import metrics_from_prices

FIXTURES = Path(__file__).parent.parent / "fixtures" / "expected_outputs"
FIXTURES.mkdir(parents=True, exist_ok=True)


def _deterministic_prices() -> pl.DataFrame:
    """A pinned synthetic dataset — 400 days, 3 symbols."""
    rows: list[dict[str, object]] = []
    start = date(2024, 1, 1)
    for sym, seed, asset_type in [
        ("BTC", 40_000.0, "crypto"),
        ("AAPL", 180.0, "stock"),
        ("C:EURUSD", 1.09, "fx"),
    ]:
        price = seed
        for i in range(400):
            d = start + timedelta(days=i)
            # deterministic walk
            price = price * (1.0 + 0.0015 * ((i % 7) - 3))
            rows.append({"symbol": sym, "asset_type": asset_type, "date": d, "close": price})
    return pl.DataFrame(rows)


def _hash_df(df: pl.DataFrame) -> str:
    csv = df.write_csv()
    return hashlib.sha256(csv.encode("utf-8")).hexdigest()


def test_returns_by_window_stable() -> None:
    prices = _deterministic_prices()
    metrics = metrics_from_prices(prices)
    as_of = date(2025, 2, 1)
    df = returns_by_window(
        metrics,
        prices,
        windows_days={"7d": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365},
        as_of=as_of,
        btc_symbol="BTC",
    ).sort(["window", "symbol"])
    golden_path = FIXTURES / "returns_by_window.csv"
    if not golden_path.exists():
        df.write_csv(golden_path)
    expected = pl.read_csv(golden_path)
    assert _hash_df(df.select(sorted(df.columns))) == _hash_df(
        expected.select(sorted(expected.columns))
    )


def test_volatility_summary_stable() -> None:
    prices = _deterministic_prices()
    metrics = metrics_from_prices(prices)
    df = volatility_summary(metrics, as_of=date(2025, 2, 1), lookback_days=365).sort("symbol")
    golden_path = FIXTURES / "volatility_summary.csv"
    if not golden_path.exists():
        df.write_csv(golden_path)
    expected = pl.read_csv(golden_path)
    # Compare numeric column with tolerance for FP drift across polars versions
    assert df["symbol"].to_list() == expected["symbol"].to_list()
    assert all(
        abs(a - b) < 1e-9
        for a, b in zip(
            df["daily_return_stdev"].to_list(), expected["daily_return_stdev"].to_list()
        )
    )


def test_dca_stable() -> None:
    prices = _deterministic_prices()
    df = compare_dca_vs_lump_sum(
        prices,
        symbol="BTC",
        monthly_amount_usd=100.0,
        months=12,
        buy_day_of_month=1,
        as_of=date(2025, 2, 1),
    ).sort("strategy")
    golden_path = FIXTURES / "dca_vs_lump.csv"
    if not golden_path.exists():
        df.write_csv(golden_path)
    expected = pl.read_csv(golden_path)
    assert df["strategy"].to_list() == expected["strategy"].to_list()
    for col in ["principal_usd", "current_value_usd", "pnl_usd", "total_return"]:
        pairs = zip(df[col].to_list(), expected[col].to_list())
        assert all(abs(a - b) < 1e-6 for a, b in pairs), f"mismatch in {col}"


def test_correlation_shape_stable() -> None:
    prices = _deterministic_prices()
    metrics = metrics_from_prices(prices)
    df = correlation_matrix(metrics, as_of=date(2025, 2, 1))
    assert df.shape == (3, 4)  # 3 rows, [symbol, BTC, AAPL, C:EURUSD]
    # Diagonal should be ~1.0
    for sym in df["symbol"].to_list():
        diag = df.filter(pl.col("symbol") == sym)[sym].item()
        assert abs(diag - 1.0) < 1e-9


def test_html_report_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end render: file exists, all six sections present, plotly.js loaded from CDN."""
    prices = _deterministic_prices()
    metrics = metrics_from_prices(prices)
    as_of = date(2025, 2, 1)

    returns_df = returns_by_window(
        metrics,
        prices,
        windows_days={"7d": 7, "1m": 30, "3m": 90},
        as_of=as_of,
        btc_symbol="BTC",
    )
    lump_rows: list[dict[str, object]] = []
    for sym in prices["symbol"].unique().to_list():
        r = lump_sum(
            prices,
            symbol=sym,
            principal_usd=1000.0,
            start=date(2024, 2, 1),
            end=as_of,
        )
        if r is not None:
            lump_rows.append(
                {
                    "symbol": r.symbol,
                    "principal_usd": r.principal_usd,
                    "units": r.units,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "start_price": r.start_price,
                    "end_price": r.end_price,
                    "current_value_usd": r.current_value_usd,
                    "pnl_usd": r.pnl_usd,
                    "total_return": r.total_return,
                }
            )
    lump_df = pl.DataFrame(lump_rows)
    dca_df = compare_dca_vs_lump_sum(
        prices, symbol="BTC", monthly_amount_usd=100.0, months=12, buy_day_of_month=1, as_of=as_of
    )
    vol_df = volatility_summary(metrics, as_of=as_of, lookback_days=365)
    corr_df = correlation_matrix(metrics, as_of=as_of)

    analysis = AnalysisConfig(
        btc_symbol="BTC",
        windows_days={"7d": 7, "1m": 30, "3m": 90},
        rolling_windows_days=[30],
        rolling_vol_days=30,
        dca=DCAConfig(btc_symbol="BTC", monthly_amount_usd=100.0, months=12, buy_day_of_month=1),
        lump_sum=LumpSumConfig(amount_usd=1000.0),
    )

    # Redirect the report output dir so the test doesn't litter the repo root.
    import analysis.html_report as hr

    monkeypatch.setattr(hr, "DATA_REPORT_DIR", tmp_path / "DATA_REPORTS")

    out_path = write_html_report(
        analysis=analysis,
        as_of=as_of,
        row_counts={"returns_by_window": returns_df.height, "lump_sum_1k": lump_df.height},
        prices=prices,
        metrics=metrics,
        returns_df=returns_df,
        lump_df=lump_df,
        dca_df=dca_df,
        vol_df=vol_df,
        corr_df=corr_df,
    )

    assert out_path.exists()
    assert out_path.name == "data_analysis.html"
    html = out_path.read_text()
    for heading in ("Q1", "Q2", "Q3", "Q4", "Correlation"):
        assert heading in html, f"missing heading: {heading}"
    # Every question panel should include its storytelling block.
    assert html.count('class="story"') >= 4
    # CDN variant: plotly loaded via <script src=…plotly…>
    assert "plotly" in html.lower()
    assert "<!doctype html>" in html.lower()
    assert len(html) < 500_000, "CDN build should be small — plotly.js should not be inlined"

    # Static twin: same directory, plotly.js inlined (~5 MB).
    static_path = out_path.with_name("data_analysis_static.html")
    assert static_path.exists(), "static fallback not written"
    static_html = static_path.read_text()
    assert "<!doctype html>" in static_html.lower()
    assert len(static_html) > 500_000, "static build should embed plotly.js"
