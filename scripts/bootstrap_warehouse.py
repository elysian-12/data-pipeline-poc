#!/usr/bin/env python
"""Apply src/pipeline/storage/schema.sql. Idempotent, near-zero cost on warm runs.

Creates: silver + silver.stg_prices, meta + meta.pipeline_runs +
meta.fact_data_quality_runs + meta.seed_fingerprints.

Does NOT run `dbt seed` — that's the transform stage's job via
`_seed_if_stale` in src/pipeline/cli.py, which fingerprint-compares seed CSVs
against meta.seed_fingerprints and skips when nothing changed. On a fresh
clone the fingerprints table is empty, so the first `make init` triggers the
seed automatically; on every subsequent `make run` the skip path makes this
script a sub-millisecond schema DDL pass (and cron calls `make run`, which
doesn't invoke bootstrap at all).
"""

from __future__ import annotations

from pipeline.config import get_settings
from pipeline.observability.logging import configure_logging, get_logger
from pipeline.observability.perf import clear_perf_log, timed
from pipeline.storage import warehouse


def main() -> None:
    configure_logging(level="INFO")
    log = get_logger(__name__)
    settings = get_settings()

    # Fresh perf log. Must happen before the first `timed()` call below.
    clear_perf_log()

    with warehouse.connect(settings) as conn, timed("bootstrap:schema"):
        warehouse.bootstrap(conn)

    log.info("warehouse.ready", duckdb_path=str(settings.duckdb_path))


if __name__ == "__main__":
    main()
