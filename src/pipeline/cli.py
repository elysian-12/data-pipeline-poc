"""Typer CLI: ingest | transform | analyze | run | backfill-gaps | doctor."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import duckdb
import typer
from rich.console import Console
from rich.table import Table

from analysis.outputs import run_analysis
from analysis.perf_report import write_perf_report
from pipeline.config import Settings, YamlConfig, get_settings, get_yaml_config
from pipeline.ingest.orchestrator import run_ingest
from pipeline.observability.logging import configure_logging, get_logger
from pipeline.observability.perf import timed
from pipeline.observability.run_tracker import RunContext, track_run
from pipeline.quality.assertions import run_silver_assertions
from pipeline.storage import warehouse

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()
log = get_logger(__name__)


def _setup() -> None:
    settings = get_settings()
    log_file = Path("logs") / f"pipeline-{datetime.now(UTC).date().isoformat()}.log"
    configure_logging(level=settings.log_level, log_file=log_file)


def _do_ingest(
    conn: duckdb.DuckDBPyConnection,
    settings: Settings,
    yaml_cfg: YamlConfig,
    start: date | None,
    end: date | None,
    ctx: RunContext,
) -> tuple[date, date, int]:
    """Inner ingest + DQ assertions. Shared by the `ingest` and `run` commands."""
    with timed("ingest:total") as meta:
        eff_start, eff_end, n = run_ingest(
            settings=settings, yaml_cfg=yaml_cfg, start=start, end=end, run_ctx=ctx
        )
        meta["silver_rows_merged"] = n
        meta["start_date"] = eff_start.isoformat()
        meta["end_date"] = eff_end.isoformat()
    with timed("ingest:dq_assertions") as dq_meta:
        results = run_silver_assertions(
            conn, quality=yaml_cfg.quality, as_of=eff_end, run_id=ctx.run_id
        )

        def _int_detail(test_name: str, key: str) -> int:
            total = 0
            for r in results:
                if r.test_name != test_name:
                    continue
                val = r.details.get(key, 0)
                if isinstance(val, int):
                    total += val
            return total

        dq_meta["duplicate_keys"] = _int_detail("silver.unique_symbol_date", "duplicate_keys")
        dq_meta["null_close_rows"] = _int_detail("silver.close_not_null", "null_close_rows")
        dq_meta["out_of_range_rows"] = _int_detail(
            "silver.close_within_bounds", "out_of_range_rows"
        )
        stale_list: list[str] = []
        for r in results:
            if r.test_name != "silver.freshness":
                continue
            raw = r.details.get("stale_symbols", [])
            if isinstance(raw, list):
                stale_list.extend(str(s) for s in raw)
        dq_meta["stale_symbols"] = stale_list
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        raise RuntimeError(f"DQ assertions failed: {[r.test_name for r in failed]}")
    return eff_start, eff_end, n


@app.command()
def ingest(
    start: Annotated[
        str | None, typer.Option(help="YYYY-MM-DD (inclusive). Omit for incremental.")
    ] = None,
    end: Annotated[
        str | None, typer.Option(help="YYYY-MM-DD (inclusive). Omit for yesterday-UTC.")
    ] = None,
) -> None:
    """Fetch from APIs → bronze Parquet → silver MERGE. Incremental by default."""
    _setup()
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    s = date.fromisoformat(start) if start else None
    e = date.fromisoformat(end) if end else None

    with warehouse.connect(settings) as conn:
        warehouse.bootstrap(conn)
        with track_run(conn) as ctx:
            eff_start, eff_end, n = _do_ingest(conn, settings, yaml_cfg, s, e, ctx)

    console.print(
        f"[green]OK[/green] ingest {eff_start.isoformat()} → {eff_end.isoformat()} "
        f"(silver rows merged: {n})"
    )


SEEDS_DIR = Path("dbt/seeds")


@app.command()
def transform() -> None:
    """Run dbt run + dbt test. `dbt seed` runs only if dbt/seeds/*.csv changed.

    Timings recorded in outputs/performance.jsonl.
    """
    _setup()
    settings = get_settings()
    _seed_if_stale(settings)
    _run_dbt(["run"])
    _run_dbt(["test"])


def _seed_if_stale(settings: Settings) -> None:
    """Run `dbt seed` only when a CSV in dbt/seeds/ differs from the bootstrap
    snapshot. Covers the sharp edge where someone edits a seed CSV and runs
    `make transform` without re-bootstrapping: the fingerprint mismatch
    triggers a re-seed and refreshes the snapshot. No-op (~5ms) on the common
    path where seeds are untouched since bootstrap.

    Opens and releases the DuckDB handle around `dbt seed` so dbt can claim
    the single-writer lock on its own adapter connection.
    """
    with warehouse.connect(settings) as conn:
        changed = warehouse.seeds_changed_since_bootstrap(conn, SEEDS_DIR)
    if not changed:
        with timed("dbt:seed") as meta:
            meta["skipped"] = True
            meta["reason"] = "fingerprints_match"
        log.info("dbt.seed.skipped", reason="fingerprints_match")
        return
    log.info("dbt.seed.stale", changed=changed)
    _run_dbt(["seed"])
    with warehouse.connect(settings) as conn:
        warehouse.write_seed_fingerprints(conn, SEEDS_DIR)


def _run_dbt(args: list[str]) -> None:
    cmd = ["dbt", *args, "--project-dir", "dbt", "--profiles-dir", "dbt"]
    log.info("dbt.invoke", cmd=" ".join(cmd))
    with timed(f"dbt:{args[0]}"):
        rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit(rc)


@app.command()
def analyze() -> None:
    """Compute Q1–Q4 outputs/ (CSV+Parquet) + DATA_REPORTS/data_analysis.html + performance_report.html."""
    _setup()
    settings = get_settings()
    yaml_cfg = get_yaml_config()
    with timed("analyze:total") as meta:
        row_counts = run_analysis(settings, yaml_cfg)
        meta["outputs"] = sum(row_counts.values())
    perf_path = write_perf_report()
    console.print(f"[green]OK[/green] analysis: {row_counts}")
    console.print(f"[green]OK[/green] perf report: {perf_path}")


@app.command()
def run() -> None:
    """Full pipeline wrapped in one track_run: ingest → dbt → analyze.

    A single run_id is threaded through every stage so meta.pipeline_runs
    captures *any* failure (ingest, dbt, or analysis), not just ingest.
    """
    _setup()
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    with warehouse.connect(settings) as conn:
        warehouse.bootstrap(conn)
        with track_run(conn) as ctx:
            eff_start, eff_end, n = _do_ingest(conn, settings, yaml_cfg, None, None, ctx)
            log.info("run.stage.dbt", run_id=ctx.run_id)
            _seed_if_stale(settings)
            _run_dbt(["run"])
            _run_dbt(["test"])
            log.info("run.stage.analyze", run_id=ctx.run_id)
            row_counts = run_analysis(settings, yaml_cfg)

    console.print(
        f"[green]OK[/green] run {eff_start.isoformat()} → {eff_end.isoformat()} "
        f"(silver: {n}, outputs: {row_counts})"
    )


@app.command("backfill-gaps")
def backfill_gaps() -> None:
    """Find missing dates per asset and fetch only those ranges."""
    _setup()
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    with warehouse.connect(settings) as conn:
        warehouse.bootstrap(conn)
        rows = conn.execute(
            """
            SELECT symbol, MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n
            FROM silver.stg_prices
            GROUP BY symbol
            """
        ).fetchall()

    if not rows:
        console.print("[yellow]silver.stg_prices is empty — run `make ingest` first[/yellow]")
        return

    earliest = min(r[1] for r in rows)
    today = datetime.now(UTC).date() - timedelta(days=1)

    with warehouse.connect(settings) as conn, track_run(conn) as ctx:
        run_ingest(
            settings=settings,
            yaml_cfg=yaml_cfg,
            start=earliest,
            end=today,
            run_ctx=ctx,
        )
    console.print(f"[green]OK[/green] backfill scanned {earliest}..{today}")


@app.command()
def doctor() -> None:
    """Print pipeline health + suggested fixes. No network calls."""
    _setup()
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    stale_running: list[tuple[str, datetime]] = []

    with warehouse.connect(settings, read_only=True) as conn:
        # Last 5 runs
        runs = conn.execute(
            """
            SELECT run_id, started_at, ended_at, status, rows_by_source, error_payload
            FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 5
            """
        ).fetchall()
        table = Table(title="Recent pipeline runs")
        for col in ("run_id", "started_at", "ended_at", "status", "rows_by_source"):
            table.add_column(col, overflow="fold")
        for r in runs:
            table.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4])[:80])
        console.print(table)

        # Orphan `running` rows older than the stale threshold = crashed run.
        # A well-behaved process updates status on exit; anything still `running`
        # past 2h was killed (SIGKILL, power loss, OOM) and deserves a warning.
        stale_cutoff = datetime.now(UTC) - timedelta(hours=2)
        stale_rows = conn.execute(
            """
            SELECT run_id, started_at
            FROM meta.pipeline_runs
            WHERE status = 'running' AND started_at < ?
            ORDER BY started_at DESC
            """,
            [stale_cutoff],
        ).fetchall()
        for rid, started in stale_rows:
            started_dt = (
                started if isinstance(started, datetime) else datetime.fromisoformat(str(started))
            )
            stale_running.append((str(rid), started_dt))

        # Latest date per symbol
        sym_tbl = conn.execute(
            """
            SELECT symbol, MAX(date) AS latest, COUNT(*) AS n
            FROM silver.stg_prices GROUP BY symbol ORDER BY symbol
            """
        ).fetchall()
        lag_tbl = Table(title="Freshness per symbol")
        for col in ("symbol", "latest", "rows", "lag_days"):
            lag_tbl.add_column(col)
        today = datetime.now(UTC).date()
        stale = []
        for sym, latest, n in sym_tbl:
            if isinstance(latest, datetime):
                latest = latest.date()
            lag = (today - latest).days if latest else None
            if lag is not None and lag > yaml_cfg.quality.freshness_max_lag_days:
                stale.append((sym, latest))
            lag_tbl.add_row(sym, str(latest), str(n), str(lag))
        console.print(lag_tbl)

        # DQ failures in last 24h
        dq = conn.execute(
            """
            SELECT test_name, run_ts, passed, row_count, severity, details
            FROM meta.fact_data_quality_runs
            WHERE run_ts > NOW() - INTERVAL 1 DAY
            ORDER BY run_ts DESC
            """
        ).fetchall()
        dq_tbl = Table(title="DQ results (last 24h)")
        for col in ("test_name", "run_ts", "passed", "row_count", "severity"):
            dq_tbl.add_column(col)
        for t, ts, passed, rc, sev, _det in dq:
            dq_tbl.add_row(t, str(ts), str(passed), str(rc), sev)
        console.print(dq_tbl)

        if stale_running:
            lines = "\n".join(
                f"  - run_id={rid} started_at={started.isoformat()}"
                for rid, started in stale_running
            )
            console.print(
                f"\n[red]STALE RUNNING[/red] ({len(stale_running)} run(s) older than 2h — "
                f"suspected crash / SIGKILL):\n{lines}\n"
                f"[yellow]SUGGESTED FIX[/yellow]: re-run `make run` — bronze writes are "
                f"idempotent, silver MERGE is no-op for existing rows."
            )

        if stale:
            start = min(s[1] for s in stale)
            end = today - timedelta(days=1)
            console.print(
                f"\n[yellow]SUGGESTED FIX[/yellow]: "
                f"`make backfill START={start.isoformat()} END={end.isoformat()}`"
            )
        else:
            console.print("\n[green]No stale symbols.[/green]")

    # dump a small JSON summary for scripting
    summary = {
        "latest_runs": [
            dict(zip(["run_id", "started", "ended", "status"], r[:4], strict=False)) for r in runs
        ],
        "stale_symbols": [s[0] for s in stale],
        "stale_running_runs": [rid for rid, _ in stale_running],
    }
    console.print(json.dumps(summary, default=str, indent=2))


if __name__ == "__main__":
    app()
