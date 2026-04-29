"""DuckDB warehouse helpers: connection, schema bootstrap, silver MERGE."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa

from pipeline.config import Settings
from pipeline.observability.logging import get_logger

log = get_logger(__name__)

SCHEMA_SQL = Path(__file__).with_name("schema.sql")


@contextmanager
def connect(settings: Settings, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection. Creates parent dir for the file if needed."""
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.duckdb_path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def bootstrap(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply schema.sql. Idempotent."""
    conn.execute(SCHEMA_SQL.read_text())
    log.debug("warehouse.bootstrap.ok", schema_sql=str(SCHEMA_SQL))


def merge_into_silver(conn: duckdb.DuckDBPyConnection, bronze_table: pa.Table) -> int:
    """Last-write-wins MERGE on (symbol, date). Returns row count of the incoming batch.

    `bronze_table` is the contents of bronze Parquet read back via
    `bronze_io.read_bronze` — see [orchestrator.run_ingest]. Tests bypass the
    parquet round-trip and pass a constructed Arrow table directly; that's
    fine because what's exercised here is the MERGE semantics, not the IO.

    Wrapped in an explicit `BEGIN TRANSACTION / COMMIT` (rollback on exception).
    The single-statement MERGE is already auto-commit atomic in DuckDB; the
    explicit transaction makes the atomicity boundary visible and future-proofs
    the call site if it ever grows to span multiple statements (e.g. per-source
    MERGE or pre-MERGE validation queries).
    """
    if bronze_table.num_rows == 0:
        return 0

    # Register the Arrow table as a DuckDB view; DuckDB reads Arrow zero-copy.
    conn.register("bronze_in", bronze_table)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            """
            INSERT INTO silver.stg_prices
                (source, asset_type, symbol, date,
                 open, high, low, close, volume, vwap, trade_count,
                 ingested_at, run_id)
            SELECT
                source, asset_type, symbol, date,
                open, high, low, close, volume, vwap, trade_count,
                ingested_at, run_id
            FROM bronze_in
            ON CONFLICT (symbol, date) DO UPDATE SET
                source       = EXCLUDED.source,
                asset_type   = EXCLUDED.asset_type,
                open         = EXCLUDED.open,
                high         = EXCLUDED.high,
                low          = EXCLUDED.low,
                close        = EXCLUDED.close,
                volume       = EXCLUDED.volume,
                vwap         = EXCLUDED.vwap,
                trade_count  = EXCLUDED.trade_count,
                ingested_at  = EXCLUDED.ingested_at,
                run_id       = EXCLUDED.run_id
            WHERE EXCLUDED.ingested_at > silver.stg_prices.ingested_at
            """
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("bronze_in")

    rows = int(bronze_table.num_rows)
    log.info("silver.merge.ok", rows=rows)
    return rows


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def seeds_changed_since_bootstrap(conn: duckdb.DuckDBPyConnection, seeds_dir: Path) -> list[str]:
    """Return seed file names whose sha256 differs from the bootstrap snapshot.

    Empty list = every seed matches what bootstrap loaded → safe to skip
    `dbt seed`. Unknown seeds (present on disk, absent in the table) count as
    changed so a freshly-added CSV always triggers a re-seed.
    """
    stored = {
        str(name): str(h)
        for name, h in conn.execute(
            "SELECT seed_name, sha256 FROM meta.seed_fingerprints"
        ).fetchall()
    }
    changed: list[str] = []
    for csv in sorted(seeds_dir.glob("*.csv")):
        if _hash_file(csv) != stored.get(csv.name):
            changed.append(csv.name)
    return changed


def write_seed_fingerprints(conn: duckdb.DuckDBPyConnection, seeds_dir: Path) -> int:
    """Snapshot sha256 of every seed CSV into meta.seed_fingerprints. Idempotent."""
    now = datetime.now(UTC)
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM meta.seed_fingerprints")
        rows = [(csv.name, _hash_file(csv), now) for csv in sorted(seeds_dir.glob("*.csv"))]
        conn.executemany(
            "INSERT INTO meta.seed_fingerprints (seed_name, sha256, updated_at) VALUES (?, ?, ?)",
            rows,
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    log.info("warehouse.seed_fingerprints.written", count=len(rows))
    return len(rows)


def latest_date_per_symbol(conn: duckdb.DuckDBPyConnection) -> dict[str, date]:
    """Return {symbol: max(date)} from silver — used by incremental ingest."""
    rows = conn.execute(
        "SELECT symbol, MAX(date) AS max_date FROM silver.stg_prices GROUP BY symbol"
    ).fetchall()
    return {str(sym): cast("date", d) for sym, d in rows}
