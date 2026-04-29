"""SQL assertions against DuckDB silver/gold — freshness, coverage, uniqueness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

from pipeline.config import QualityConfig
from pipeline.observability.logging import get_logger
from pipeline.observability.run_tracker import record_dq_result

log = get_logger(__name__)


@dataclass
class AssertionResult:
    test_name: str
    passed: bool
    row_count: int
    severity: str
    details: dict[str, object]


def _scalar_int(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run a SQL returning a single COUNT/INT scalar. Returns 0 on NULL/missing row."""
    row = conn.execute(sql).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def run_silver_assertions(
    conn: duckdb.DuckDBPyConnection,
    *,
    quality: QualityConfig,
    as_of: date | None = None,
    run_id: str | None = None,
) -> list[AssertionResult]:
    """Run the fail-the-pipeline DQ checks over silver.

    Persists each check to meta.fact_data_quality_runs. Returns the results so
    the caller can decide whether to raise.
    """
    as_of = as_of or date.today()
    results: list[AssertionResult] = []

    # 1. Primary-key uniqueness
    dupes = _scalar_int(
        conn,
        """
        SELECT COUNT(*) FROM (
          SELECT symbol, date, COUNT(*) AS n
          FROM silver.stg_prices GROUP BY symbol, date HAVING COUNT(*) > 1
        )
        """,
    )
    results.append(
        AssertionResult(
            test_name="silver.unique_symbol_date",
            passed=(dupes == 0),
            row_count=dupes,
            severity="error",
            details={"duplicate_keys": dupes},
        )
    )

    # 2. close not null
    null_close = _scalar_int(conn, "SELECT COUNT(*) FROM silver.stg_prices WHERE close IS NULL")
    results.append(
        AssertionResult(
            test_name="silver.close_not_null",
            passed=(null_close == 0),
            row_count=null_close,
            severity="error",
            details={"null_close_rows": null_close},
        )
    )

    # 3. close within configured bounds
    oob = _scalar_int(
        conn,
        f"""
        SELECT COUNT(*) FROM silver.stg_prices
        WHERE close < {quality.close_lower_bound}
           OR close > {quality.close_upper_bound}
        """,
    )
    results.append(
        AssertionResult(
            test_name="silver.close_within_bounds",
            passed=(oob == 0),
            row_count=oob,
            severity="error",
            details={
                "out_of_range_rows": oob,
                "bounds": [quality.close_lower_bound, quality.close_upper_bound],
            },
        )
    )

    # 4. freshness per symbol
    stale = conn.execute(
        f"""
        SELECT symbol, MAX(date) AS max_date
        FROM silver.stg_prices
        GROUP BY symbol
        HAVING DATE_DIFF('day', MAX(date), DATE '{as_of.isoformat()}')
               > {quality.freshness_max_lag_days}
        """
    ).fetchall()
    results.append(
        AssertionResult(
            test_name="silver.freshness",
            passed=(len(stale) == 0),
            row_count=len(stale),
            severity="error" if stale else "info",
            details={"stale_symbols": [s[0] for s in stale]},
        )
    )

    for r in results:
        record_dq_result(
            conn,
            test_name=r.test_name,
            passed=r.passed,
            row_count=r.row_count,
            severity=r.severity,
            run_id=run_id,
            details=r.details,
        )
        (log.info if r.passed else log.error)(
            "dq.result",
            test=r.test_name,
            passed=r.passed,
            row_count=r.row_count,
            details=r.details,
        )

    return results
