"""Shared fixtures: a tiny in-memory price dataset reused by analysis tests."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pytest

from pipeline.config import Settings
from pipeline.storage import warehouse
from tests._helpers import metrics_from_prices

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_prices() -> pl.DataFrame:
    """Three symbols, 60 days each, deterministic prices."""
    rows: list[dict[str, object]] = []
    start = date(2025, 1, 1)
    for sym, seed, asset_type in [
        ("BTC", 100_000.0, "crypto"),
        ("AAPL", 180.0, "stock"),
        ("C:EURUSD", 1.09, "fx"),
    ]:
        price = seed
        for i in range(60):
            d = start + timedelta(days=i)
            # Zig-zag walk — deterministic so tests are stable.
            price = price * (1.0 + 0.002 * ((i % 5) - 2))
            rows.append({"symbol": sym, "asset_type": asset_type, "date": d, "close": price})
    return pl.DataFrame(rows)


@pytest.fixture
def sample_metrics(sample_prices: pl.DataFrame) -> pl.DataFrame:
    """Synthesize `gold.fact_daily_metrics` from `sample_prices` for analysis-layer
    tests. Mirrors the dbt model: daily/log returns and calendar-day rolling
    returns over {7, 30, 90, 180, 365} days, plus 30-day rolling stdev.
    """
    return metrics_from_prices(sample_prices)


@pytest.fixture
def tmp_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings pointing at a temp DuckDB + bronze dir."""
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("BRONZE_URI", str(tmp_path / "bronze"))
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    # Bust the lru_cache on get_settings
    from pipeline import config

    config.get_settings.cache_clear()
    return config.get_settings()


@pytest.fixture
def warehouse_conn(tmp_settings: Settings):
    """Connected, bootstrapped DuckDB instance."""
    with warehouse.connect(tmp_settings) as conn:
        warehouse.bootstrap(conn)
        yield conn


def _bronze_row(
    symbol: str, d: date, close: float, ingested_at: datetime, run_id: str
) -> dict[str, object]:
    return {
        "source": "massive" if symbol != "BTC" else "coingecko",
        "asset_type": "stock" if symbol == "AAPL" else "crypto",
        "symbol": symbol,
        "date": d,
        "open": close * 0.99,
        "high": close * 1.01,
        "low": close * 0.98,
        "close": close,
        "volume": 1_000_000.0,
        "vwap": close,
        "trade_count": 10,
        "ingested_at": ingested_at,
        "run_id": run_id,
    }


@pytest.fixture
def silver_seeder():
    """Helper: MERGE a list of bronze rows into silver.stg_prices of a given conn."""

    def _seed(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, object]]) -> None:
        table = pa.Table.from_pylist(rows)
        warehouse.merge_into_silver(conn, table)

    return _seed


@pytest.fixture
def make_bronze_row():
    """Factory for minimal bronze rows."""
    return _bronze_row
