"""Bronze layer: deterministic paths + overwrite on re-run (idempotent storage)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq

from pipeline.config import Settings
from pipeline.models import BronzeRow
from pipeline.storage import bronze as bronze_io


def _read_file(path: str) -> pq.Table:
    # Read a single Parquet file without Hive-partition auto-discovery
    # (our paths contain "=" in segment names, which pyarrow would otherwise
    # interpret as a partitioned dataset).
    return pq.ParquetFile(path).read()


def _row(symbol: str, d: date, close: float) -> BronzeRow:
    return BronzeRow(
        source="massive",
        asset_type="stock",
        symbol=symbol,
        date=d,
        open=close * 0.99,
        high=close * 1.01,
        low=close * 0.98,
        close=close,
        volume=1.0,
        vwap=close,
        trade_count=1,
        ingested_at=datetime.now(UTC),
        run_id="test-run",
    )


def test_bronze_path_is_deterministic(tmp_settings: Settings) -> None:
    ingested = date(2026, 4, 19)
    p1 = bronze_io.bronze_path(
        tmp_settings, source="massive", asset_type="stock", ingested_date=ingested
    )
    p2 = bronze_io.bronze_path(
        tmp_settings, source="massive", asset_type="stock", ingested_date=ingested
    )
    assert p1 == p2
    assert "source=massive" in p1
    assert "asset_type=stock" in p1
    assert "ingested_date=2026-04-19" in p1


def test_bronze_write_overwrites_on_rerun(tmp_settings: Settings) -> None:
    ingested = date(2026, 4, 19)
    path = bronze_io.bronze_path(
        tmp_settings, source="massive", asset_type="stock", ingested_date=ingested
    )
    # First write — 1 row
    bronze_io.write_bronze([_row("AAPL", date(2026, 4, 18), 100.0)], tmp_settings, path)
    assert Path(path).exists()
    table_1 = _read_file(path)
    assert table_1.num_rows == 1

    # Second write — 2 rows. Must overwrite, not duplicate.
    bronze_io.write_bronze(
        [_row("AAPL", date(2026, 4, 17), 99.0), _row("AAPL", date(2026, 4, 18), 101.0)],
        tmp_settings,
        path,
    )
    table_2 = _read_file(path)
    assert table_2.num_rows == 2
    closes = table_2.column("close").to_pylist()
    assert sorted(closes) == [99.0, 101.0]
