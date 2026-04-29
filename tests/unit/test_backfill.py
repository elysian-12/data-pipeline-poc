"""MERGE idempotency: re-runs are no-ops; newer ingested_at overwrites."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow as pa

from pipeline.storage import warehouse


def _rows(
    ingested_at: datetime, close: float = 100.0, run_id: str = "r1"
) -> list[dict[str, object]]:
    return [
        {
            "source": "massive",
            "asset_type": "stock",
            "symbol": "AAPL",
            "date": date(2025, 1, 3),
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": close,
            "volume": 1_000_000.0,
            "vwap": 100.0,
            "trade_count": 10,
            "ingested_at": ingested_at,
            "run_id": run_id,
        },
    ]


def test_merge_identical_batch_is_no_op(warehouse_conn: duckdb.DuckDBPyConnection) -> None:
    ts = datetime(2025, 1, 4, 1, 0, tzinfo=UTC)
    batch = pa.Table.from_pylist(_rows(ts))
    warehouse.merge_into_silver(warehouse_conn, batch)
    count_after_first = warehouse_conn.execute("SELECT COUNT(*) FROM silver.stg_prices").fetchone()[
        0
    ]
    warehouse.merge_into_silver(warehouse_conn, batch)
    count_after_second = warehouse_conn.execute(
        "SELECT COUNT(*) FROM silver.stg_prices"
    ).fetchone()[0]
    assert count_after_first == count_after_second == 1


def test_merge_newer_ingested_at_overwrites(
    warehouse_conn: duckdb.DuckDBPyConnection,
) -> None:
    earlier = datetime(2025, 1, 4, 1, 0, tzinfo=UTC)
    later = earlier + timedelta(hours=1)
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(_rows(earlier, close=100.0)))
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(_rows(later, close=105.0)))
    row = warehouse_conn.execute(
        "SELECT close, ingested_at FROM silver.stg_prices WHERE symbol = 'AAPL'"
    ).fetchone()
    assert row[0] == 105.0


def test_merge_older_ingested_at_is_ignored(
    warehouse_conn: duckdb.DuckDBPyConnection,
) -> None:
    later = datetime(2025, 1, 4, 1, 0, tzinfo=UTC)
    earlier = later - timedelta(hours=1)
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(_rows(later, close=100.0)))
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(_rows(earlier, close=99.0)))
    row = warehouse_conn.execute(
        "SELECT close FROM silver.stg_prices WHERE symbol = 'AAPL'"
    ).fetchone()
    assert row[0] == 100.0


def test_gap_fill_restores_deleted_rows(
    warehouse_conn: duckdb.DuckDBPyConnection,
) -> None:
    base_ts = datetime(2025, 1, 4, 1, 0, tzinfo=UTC)
    # Seed 3 consecutive days
    rows = [
        {
            "source": "massive",
            "asset_type": "stock",
            "symbol": "AAPL",
            "date": date(2025, 1, d),
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0 + d,
            "volume": 1.0,
            "vwap": 100.0,
            "trade_count": 1,
            "ingested_at": base_ts,
            "run_id": "r1",
        }
        for d in (3, 4, 5)
    ]
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(rows))
    # Delete the middle
    warehouse_conn.execute("DELETE FROM silver.stg_prices WHERE date = DATE '2025-01-04'")
    assert warehouse_conn.execute("SELECT COUNT(*) FROM silver.stg_prices").fetchone()[0] == 2
    # Re-ingest with a later ingested_at → the missing row is inserted
    later_ts = base_ts + timedelta(hours=1)
    for r in rows:
        r["ingested_at"] = later_ts
    warehouse.merge_into_silver(warehouse_conn, pa.Table.from_pylist(rows))
    assert warehouse_conn.execute("SELECT COUNT(*) FROM silver.stg_prices").fetchone()[0] == 3
