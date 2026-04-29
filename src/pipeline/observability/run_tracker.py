"""Write pipeline run records to meta.pipeline_runs + meta.fact_data_quality_runs."""

from __future__ import annotations

import json
import os
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ulid import ULID

from pipeline.observability.logging import get_logger

if TYPE_CHECKING:
    import duckdb

log = get_logger(__name__)


@dataclass
class RunContext:
    """Collects state for a pipeline invocation; flushed to meta on exit."""

    run_id: str = field(default_factory=lambda: str(ULID()))
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None
    status: str = "running"
    rows_by_source: dict[str, int] = field(default_factory=dict)
    error_payload: str | None = None
    git_sha: str | None = field(default_factory=lambda: os.environ.get("GIT_SHA"))

    def add_rows(self, source: str, n: int) -> None:
        self.rows_by_source[source] = self.rows_by_source.get(source, 0) + n


@contextmanager
def track_run(conn: duckdb.DuckDBPyConnection) -> Iterator[RunContext]:
    """Wrap a run. Writes `meta.pipeline_runs` with a `running` heartbeat on entry,
    then updates to `success` or `failed` on exit. A hard kill (SIGKILL, power
    loss) leaves an orphan `running` row — `pipeline doctor` surfaces those so a
    crashed run never disappears silently.
    """
    ctx = RunContext()
    log.info("run.started", run_id=ctx.run_id, started_at=ctx.started_at.isoformat())
    _insert_running(conn, ctx)
    try:
        yield ctx
        ctx.status = "success"
    except Exception as exc:
        ctx.status = "failed"
        ctx.error_payload = "".join(traceback.format_exception(exc))
        log.error("run.failed", run_id=ctx.run_id, error=str(exc))
        ctx.ended_at = datetime.now(UTC)
        _finalize_run(conn, ctx)
        raise
    finally:
        if ctx.status == "success":
            ctx.ended_at = datetime.now(UTC)
            _finalize_run(conn, ctx)
            log.info(
                "run.completed",
                run_id=ctx.run_id,
                status=ctx.status,
                rows_by_source=ctx.rows_by_source,
                duration_s=(ctx.ended_at - ctx.started_at).total_seconds(),
            )


def _insert_running(conn: duckdb.DuckDBPyConnection, ctx: RunContext) -> None:
    conn.execute(
        """
        INSERT INTO meta.pipeline_runs
            (run_id, started_at, ended_at, status, rows_by_source, error_payload, git_sha)
        VALUES (?, ?, NULL, 'running', NULL, NULL, ?)
        """,
        [ctx.run_id, ctx.started_at, ctx.git_sha],
    )


def _finalize_run(conn: duckdb.DuckDBPyConnection, ctx: RunContext) -> None:
    conn.execute(
        """
        UPDATE meta.pipeline_runs
        SET ended_at       = ?,
            status         = ?,
            rows_by_source = ?,
            error_payload  = ?
        WHERE run_id = ?
        """,
        [
            ctx.ended_at,
            ctx.status,
            json.dumps(ctx.rows_by_source),
            ctx.error_payload,
            ctx.run_id,
        ],
    )


def record_dq_result(
    conn: duckdb.DuckDBPyConnection,
    *,
    test_name: str,
    passed: bool,
    row_count: int,
    severity: str = "error",
    run_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO meta.fact_data_quality_runs
            (run_id, test_name, run_ts, passed, row_count, severity, details)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            test_name,
            datetime.now(UTC),
            passed,
            row_count,
            severity,
            json.dumps(details or {}),
        ],
    )
