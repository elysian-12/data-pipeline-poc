"""Atomicity guarantees: bronze tmp+rename, run-tracker heartbeat."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.config import Settings
from pipeline.models import BronzeRow
from pipeline.observability.run_tracker import track_run
from pipeline.storage import bronze as bronze_io


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


def _list_tmp_files(dir_path: Path) -> list[Path]:
    return [p for p in dir_path.iterdir() if ".tmp." in p.name]


def test_bronze_write_leaves_no_tmp_file_on_success(tmp_settings: Settings) -> None:
    """Happy path: after write, only the final data.parquet exists."""
    path = bronze_io.bronze_path(
        tmp_settings, source="massive", asset_type="stock", ingested_date=date(2026, 4, 19)
    )
    bronze_io.write_bronze([_row("AAPL", date(2026, 4, 18), 100.0)], tmp_settings, path)

    final = Path(path)
    assert final.exists()
    assert pq.ParquetFile(str(final)).read().num_rows == 1
    assert _list_tmp_files(final.parent) == []


def test_bronze_write_crashes_cleanly(
    tmp_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate a crash mid-write: no final file, no tmp file left behind."""
    path = bronze_io.bronze_path(
        tmp_settings, source="massive", asset_type="stock", ingested_date=date(2026, 4, 19)
    )

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated crash mid-write")

    monkeypatch.setattr(bronze_io.pq, "write_table", _boom)

    with pytest.raises(RuntimeError, match="simulated crash"):
        bronze_io.write_bronze([_row("AAPL", date(2026, 4, 18), 100.0)], tmp_settings, path)

    final = Path(path)
    assert not final.exists(), "final path must not exist after a failed write"
    # The directory was created by makedirs; just assert no tmp debris.
    if final.parent.exists():
        assert _list_tmp_files(final.parent) == []


def test_track_run_writes_running_then_success(warehouse_conn) -> None:  # type: ignore[no-untyped-def]
    """A successful run writes two states: `running` on entry, `success` on exit."""
    conn = warehouse_conn

    with track_run(conn) as ctx:
        # Inside the block, exactly one row exists with status='running'.
        rows = conn.execute(
            "SELECT status, ended_at FROM meta.pipeline_runs WHERE run_id = ?",
            [ctx.run_id],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "running"
        assert rows[0][1] is None  # ended_at not set during the run

    # After the context exits, the same row has been updated to success + has ended_at.
    rows = conn.execute(
        "SELECT status, ended_at FROM meta.pipeline_runs WHERE run_id = ?",
        [ctx.run_id],
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "success"
    assert rows[0][1] is not None


def test_track_run_marks_failed_on_exception(warehouse_conn) -> None:  # type: ignore[no-untyped-def]
    """A raised exception inside the block flips status to `failed` + records the trace."""
    conn = warehouse_conn

    with pytest.raises(ValueError, match="boom"), track_run(conn) as ctx:
        raise ValueError("boom")

    rows = conn.execute(
        "SELECT status, error_payload FROM meta.pipeline_runs WHERE run_id = ?",
        [ctx.run_id],
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "failed"
    assert rows[0][1] is not None
    assert "boom" in rows[0][1]
